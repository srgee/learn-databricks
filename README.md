
# Werable Data Processor over DataLakehouse

An end-to-end streaming medallion pipeline (Bronze → Silver → Gold) that ingests IoT and application data from a fictional gym chain (user registrations, gym check-ins, and a Kafka event stream user profiles, workouts, heart-rate telemetry).

Built to run entirely on Databricks Free Edition through Databricks Connect (Python),and deliberately structured like a production repository to serve as a learning project on applying software engineering best practices to modern data engineering.

No paid Databricks account is required. Every design decision in this repo (path formats, checkpoint locations, schema handling) accounts for the constraints of the free tier instead of fighting them.

# What will you learn?

- Real architecture, toy-sized data. The full medallion pattern (Auto Loader,CDC streaming, MERGE upserts, streaming aggregations) on a dataset small enough to reprocess in seconds.
- Production-shaped repository. Packaging (Python wheel), Asset Bundles,configuration with a documented priority chain, and tests. The engineering practices that tutorials usually skip.
- Every layer is observable. With Unity Catalog you can watch raw files become bronze tables, then curated silver entities, then gold aggregates. All fromCatalog Explorer.
- Documented failure modes. The compatibility playbookbelow records real errors hit while building this, so you learn why the rulesexist, not just the rules.

# Engineering highlights
- Specs vs. engines. SourceFeed / StreamSpec / MergeSpec dataclasses describe what flows; the two engines define how. Custom build_df builderscover the irregular cases (stream–stream joins, aggregations).
- One source of configuration truth. A frozen Settings dataclass declares eachsetting once (flag, env var, default); the CLI parser is generated from it.Resolution order: CLI > environment > .env > defaults.
- MERGE safety policies. Every target declares its update semantics:insert-only (immutable facts), last-writer-wins (user_profile),SCD Type 1 (user_bins), monotonic advance (gym_logs). All MERGEs areidempotent by business key, which is what makes replays and backfills safe.
- Explicit phase orchestration. Dependent targets run in declared phases(PHASES); a phase fully drains before the next starts.
- Two trigger modes. Default availableNow (drain everything, exit — ideal onmetered/free compute) or continuous --stream --processing-time 30 seconds.

# Prerequisites
- A Databricks Free Edition account
- Python
- Databricks Connect (la version que usamos es anituguua. Esta impuesta por el runtime de databricks. Busca URL para documentar esto)
- Databricks CLI
- Existing Databricks workspace, catalogue and volume
- History and lookup data in Databricks volume (date-lookup.json available in the repo)

# Setup
git clone <repo-url> && cd <repo>uv venv && source .venv/bin/activate      # or python -m venvuv pip install -e ".[dev]"                # editable install incl. databricks-connect
Authenticate once — Databricks Connect picks it up:

databricks auth login --host https://<your-workspace>.databricks.com
Copy .env.example to .env and adjust (DATABRICKS_CONFIG_PROFILE, GYM_ENV, ...).Keep .env out of version control — it is gitignored by default.


# Load data & deploy schema
sbit load-history          # uploads raw fixtures + builds date_lookup (idempotent)sbit deploy                # applies sql/ DDL: schema, tables, volumes
Run the pipeline
sbit --layer bronze        # Auto Loader: raw files → bronze (availableNow)sbit --layer silver        # CDC streams + MERGE: bronze → silversbit --layer gold          # aggregation: silver → gold
Or run it as a Databricks Job
databricks bundle deploy   # uploads the wheel and the job definitiondatabricks bundle run sbit_pipeline
The job chains the tasks in dependency order: deploy_ddl → bronze → silver → gold.

# Configuration
Every setting follows the same priority chain — CLI beats environment, environmentbeats .env, .env beats defaults:

Aquí tienes la información organizada en una tabla Markdown:

| Flag | Env var | Default | Purpose |
| --- | --- | --- | --- |
| `--env` | `GYM_ENV` | `dev` | Unity Catalog (catalog name) to run against |
| `--layer` | `GYM_LAYER` | `bronze` | `bronze` | `silver` | `gold` |
| `--stream` | `GYM_STREAM` | `off` | continuous mode instead of `availableNow` |
| `--processing-time` | `GYM_PROCESSING_TIME` | `5 seconds` | trigger interval in continuous mode |
| `--starting-version` | `GYM_STARTING_VERSION` | `0` | Delta version to read from (only with an empty checkpoint — for backfills) |


# Next steps


Proyecto de aprendiza Databricks. Se basa en capstoone project del curso de Udemy Streaming Processin con Databricks.

Este proyecto aplica las mejores practicas de ingenieria del softwar para el procesamiento de datos en streaming con Databricks..

El proyecto asume la existencia de un catalogo en Databricks, no crea un catalogo nuevo.

Best practices recommend use separate databases for bronze, silver and gold. For simplicity, this project uses a single databse for all medallion layers.

To avoid JVM dependency locally, we use Databricks Connect to use cloud resources to run the code from the dev environment. This requires to swap
PySpark for databricks-connect in our dev dependencies

As per Databricks Connect version support matrix, for serverless compute we need:
- Python 3.12
- "databricks-connect>=18.0,<18.4"

The project is tested on serverless only. Note that there are some API methods that are not available on serverles.

SQL DDL for structure, DataFrame API for data. Reason: SQL for data transformations misses typing and testing capabilities.

Databricks permite la integración con una capa de almacenamiento externa (cloud storage) para guardar tanto el metastore del catálogo como los datos (tablas, vistas, archivos, etc). Por simplicidad, el proyecto usa default storage. En Databricks, Default storage is a fully managed object storage platform that provides ready-to-use storage.

Serverless workspaces use default storage for internal and workspace storage, and for the default catalog that gets created with the workspace. In serverless workspaces, you can create additional catalogs in either default storage or in your own cloud object storage.

In both classic workspaces and serverless workspaces, default storage is used by features to store things like control plane metadata, derived data, models, and other artifacts

By default, catalogs that use default storage are only accessible from the workspace where they are created.

El proyecto asume la existencia de un catalogo dedicado pre-existente en Databricks.

El proyecto crea un volumen (VOLUME_NAME) para el almacenamiento de datos no tabulares, incluyendo la siguiente estructura:
- data_zone (non-tabular data)
- checkpoint_zone (checkpoint storage)

Dual session:
- local or CI/CD running on databricks-connect
- prod runns on Databricks Runtime (DBR)

All business logic implemented as pure functions over DataFrames (see example below)

Sample entry point invoked by job in prod. The code does not know anything about the runtime (dev or prod).
The environment is read from the command line (argument)


ESTRUCTURA PROYECTO:

learn-databricks/
├── databricks.yml              # config del bundle (targets, variables, entrypoints)
├── resources/
│   └── jobs.py                 # definición de jobs con databricks-bundles
├── src/
│   └── sbit/
│       ├── __init__.py
│       ├── config.py           # configuración por entorno (rutas e identificadores)
│       ├── bronze_specs.py     # QUE ingestar (3 fuentes)
│       ├── main.py             # punto de entrada (CLI): parsea args, construye sesión, elige capa
│       ├── runtime.py          # fábrica de sesión dual (patrón clave)
│       ├── bronze.py           # motor genérico de ingesta de datos crudos (el "como")
│       ├── transforms.py       # transformaciones reusables: funciones puras sobre DataFrames (el "cómo")
│       ├── streaming.py        # CÓMO hacer streaming+MERGE para gold y silver
│       ├── upserters.py        # CÓMO hacer el MERGE (MergeSpec, merge_batch, dedupe_latest)
│       ├── silver_specs.py     # QUE transformar (8 targets) + fases
│       ├── transforms.py       # transformaciones reusables: funciones puras sobre DataFrames (df -> df)
│       └── gold_specs.py       # QUE agregar (1 target)
├── tests/
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── pyproject.toml
└── .github/workflows/ci-cd.yml

Las SPECS son un registro declarativo de fuentes (el "qué"). Indican qué tablas existen, de donde vienen, qué esquema y qué opciones. Son básicamente datos, no tienen lógica. Existe una spec por capa.

Usamos dos motores por que los patrones son distintos:
- Bronze: lee archivos, escribe directamente sin MERGE (append)
- Silver/Gold: lee tablas Delta con opciones CDC, escriben via foreachBatch + DeltaTable.merge

Las transformaciones son funciones puras que no leen estado global, no deciden rutas, ni tocan la sesion. Por eso son testables sin necesityar infraestructura y son 100% reutilizables entre las capas.

El DDL no pertenece ni a los specs (declaran qué datos fluyen) ni a los motores (ejecutan cómo). Es infraestructura: se versiona en su propio sitio y se ejecuta como task previo en el bundle, antes de bronze. Hay además una razón técnica que lo hace obligatorio en tu proyecto:

Bronze se auto-crea: toTable() crea la tabla si no existe con el schema del stream. En nuestro proyecto mantenemoos el DDL en la capa de bronze tambien. De esta forma cualquier tarea puede ejecutarse en cualquier orden.

Usamos una carpeta sql para almacenar los scripts de DDL. DDL idempotente por capa, un deploy.py que lo aplica en orden, y un task deploy_ddl como primer eslabón del job, con un test de esquema que vigile el drift. Tenemos el problema de que el esquema de cada tabla vive en 2 sitios distintos (el string del spec y el fichero SQL). Si cambias uno y no el otro, el MERGE fallará en runtime con un error poco descriptivo. Dos defensas:

1. Test de esquema que verifique las columnas de cada tabla destino contra lo esperado
2. A medio plazo generar los .sql desde las specs mediante un script. Los specs pasarían a ser la única fuente de verdad

Cuándo necesitarías algo más
- Evolución de esquema con equipo: cuando empieces a hacer ALTER TABLE ADD COLUMN sobre tablas en producción, el DDL idempotente no basta; ahí entran migraciones versionadas (Flyway/Liquibase vía JDBC, o simplemente ficheros sql/YYYYMMDD_descripcion.sql que aplican deltas). Overkill para Free Edition hoy.
- dbt: si silver/gold migraran a proceso batch, dbt-databricks generaría el DDL por ti. Con streaming y foreachBatch(MERGE) no encaja directamente; no lo cambies ahora.

```toml
# sample pyproject.toml: databricks-connect only exists on dev, Databricks Runtime already provides pyspark and base deps
[project]
name = "etl_ventas"
version = "1.0.0"
requires-python = ">=3.11"
dependencies = []                # el DBR ya provee pyspark y librerías base

[project.entry-points."etl_ventas"]
main = "etl_ventas.jobs:main"    # entry point para el python_wheel_task

[project.optional-dependencies]
dev = [
    "databricks-connect==15.4.*",   # ⚠️ pinned a la versión del DBR; SOLO dev
    "databricks-bundles>=1.20,<2",
    "pytest>=8",
    "chispa",                       # asserts sobre DataFrames
    "ruff",
    "mypy",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]
```
Pasos:
1. Autenticacion interactiva (una sólo vez en dev): ejemplo `databricks auth login --host https://adb-123456789.azuredatabricks.net`. Esto crea un perfil en ~/.databrickscfg
2. Añade `serverless_compute_id = auto` a el archivo ~/.databrickscfg para que el código sea válido tanto para prod como para dev
3. Parametros de configuracion: config dataclass
4. Crear tablas (DDL): setup helper class
5. Cargar datos historicos y lookup (solo se carga una vez antes de pasar a produccion):
    - Carga de datos estaticos de búsqueda en la tabla date_lookup de la capa silver (date dimension). En proyectos reales, se cargan atributos temporales para 20 años de acuerdo a los estandares de la industria para este tipo de tablas. Por simplicidad, usaremos una dimensión temporal reducida (año 2023).
    - Por simplicidad, no usaremos datos históricos para el proyecto. Normalmente, la mayoría de proyectos del mundo real requerirán integrar los datos históricos, por lo que migrar los datos existentes y cargarlos es una tarea habitual (1. subir archivos a la nube, 2. load the data to DB)
6. Ingestión de datos (bronze layer): existen dos tipos de pipelines que podemos usar para ingestar los datos de origen (raw data) en nuestra capa bronce:
    - Batch
    - Streaming <== RECOMENDADO

Ventajas de procesamiento continuo (streaming):
- Soporta procesamiento por lotes sin cambiar el código, simplemente se ejecuta a intervalos regulares
- Gestión automática de checkpoints
- Reinicio automatico en caso de fallo

Se espera que la tabla kafka_multiplex_bz almacena datos masivos por lo que se recomiendo realizar particiones. En nuestro caso, enriquecemos la tabla con los datos de la tabla date_lookup para poder particionar la tabla kafka_multiplex_bz por semanas.

En lugar de implementar una función por cada tabla de la capa Bronze, adoptamos un diseño dirigido por datos. Cada fuente de datos es un dato, no un método. Esto nos permite eliminar redundancia en el código y facilita la mantenibilidad. La forma de recolectar datos es muy similar en este capa.


```yaml
# FASE 4
# databricks.yml
bundle:
  name: etl-ventas

python:
  loads:
    - 'resources.jobs:load'      # entrypoint: ejecuta load() y fusiona el resultado

variables:
  catalog_prefix:
    description: Prefijo del catálogo UC por entorno
    default: dev

targets:
  dev:
    default: true
    workspace:
      host: https://adb-123456789.azuredatabricks.net
      artifact_path: /Volumes/main/_artifacts/etl_ventas/dev   # wheel en UC Volume

  prod:
    mode: production             # exige deploy con SP, aísla recursos, protege contra
    workspace:                   # despliegues accidentales desde local
      host: https://adb-123456789.azuredatabricks.net
      artifact_path: /Volumes/main/_artifacts/etl_ventas/prod
    variables:
      catalog_prefix: prod
    run_as:
      service_principal_name: sp-etl-ventas-prod
```

```yaml
# resources/jobs.py
from databricks.bundles.jobs import (
    ClusterSpec, CronSchedule, Job, JobCluster, PythonWheelTask, Task,
)


def load() -> list[Job]:
    """Genera un job por tabla — el caso de uso donde Python brilla vs YAML."""
    return [_job(t) for t in ("customers", "orders", "payments")]


def _job(table: str) -> Job:
    return Job(
        name=f"etl_ventas_{table}",
        schedule=CronSchedule(
            quartz_cron_expression="0 0 5 * * ?",
            timezone_id="Europe/Madrid",
        ),
        parameters={"catalog": "${vars.catalog_prefix}"},   # interpolación resuelta en deploy
        tasks=[
            Task(
                task_key="ingest",
                python_wheel_task=PythonWheelTask(
                    package_name="etl_ventas",
                    entry_point="main",
                    parameters=["--catalog", "${vars.catalog_prefix}",
                                "--table", table],
                ),
            ),
        ],
        job_clusters=[
            JobCluster(
                job_cluster_key="main",
                new_cluster=ClusterSpec(
                    spark_version="15.4.x-scala2.12",
                    node_type_id="Standard_DS3_v2",
                    num_workers=2,
                    data_security_mode="SINGLE_USER",
                ),
            )
        ],
    )
```

FASE 5:
```bash
# Validar la config generada por Python antes de nada
databricks bundle validate -t dev

# Desplegar a dev y hacer smoke test manual
databricks bundle deploy -t dev
databricks bundle run etl_ventas_customers -t dev
````
FASE 6: CI/CD
```yaml
# .github/workflows/ci-cd.yml
name: ci-cd

on:
  pull_request:
  push:
    branches: [main]

env:
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}

jobs:
  quality:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: ruff check src tests
      - run: mypy src
      - run: pytest tests/unit
      - run: databricks bundle validate -t dev
        env:
          DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}

  deploy-dev:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: pytest tests/unit
      - run: databricks bundle deploy -t dev
        env:
          DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}

  deploy-prod:
    needs: deploy-dev
    runs-on: ubuntu-latest
    environment: production     # ← exige aprobación manual en GitHub
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_CLIENT_ID: ${{ secrets.SP_PROD_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_PROD_CLIENT_SECRET }}
```

Despliegue a produccion con aprobacion manual: `deploy-prod model=manual`

OAuth M2M con service principals (nunca PATs de usuario). prod sólo tiene los permisos necesarios (run_as)

En prod, el origen de la coonfiguracion son las variables y parametros del bundle. En dev el origen de la config es la sehell o .env

El wheel desplegado en prod se construye a partir del commit de main no de un artefacto subido a mano

# Validar la config generada por Python antes de nada
databricks bundle validate -t dev

# Desplegar a dev y hacer smoke test manual
databricks bundle deploy -t dev
databricks bundle run etl_ventas_customers -t dev


MEJORA:
14/10: para proyecto de aprendizaje mantenemos el aprovisionamiento de recursos via SQL DDL. Se deja como mejora la integración con herramientas IaC
como Terraform o Pulumi. Sin embargo, queda pendiente la refactorizacion para implementar el layout profesional. En este caso, debemos sacar toda la logica DDL de la aplicacion para que se invoque coo un paso mas del CI antes del bundle deploy. Para ello debemos mover todo el codigo DDL a un modulo bootstrap.py en la carpeta raiz. También creamos un nuevo modulo preconditions.py dentro de "lib" para las validaciones (valida los recursos de infra necesarios para el proyecto).

MEJORA: sustituir print por logging

SPARK CONFIG:

* **`spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)`**
Ajusta el número de particiones tras un *shuffle* (como en *joins* o agregaciones) para que coincida con el nivel de paralelismo por defecto del clúster (`sc.defaultParallelism`). Evita usar el valor por defecto de Spark (200), que suele ser ineficiente para datasets pequeños o medianos.
* **`spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)`**
Activa la escritura optimizada en tablas Delta. Agrupa y reorganiza las particiones de forma dinámica antes de escribir en disco para evitar generar miles de archivos pequeños (*small files problem*).
* **`spark.databricks.delta.autoCompact.enabled`**
Habilita la compactación automática en Delta Lake. Tras una escritura, comprueba si hay archivos pequeños y los fusiona en archivos más grandes de manera automática, reduciendo la necesidad de ejecutar comandos `OPTIMIZE` manuales.
* **`spark.sql.streaming.stateStore.providerClass`**
Configura **RocksDB** como el proveedor del almacén de estado para *Spark Structured Streaming*. Es ideal para flujos con mucho estado (como *joins* o agregaciones en streaming), ya que gestiona los datos fuera de la memoria JVM principal, evitando errores de falta de memoria (*OOM*).



------------------------------------------------------------------------------------------------------------------
TODO (19/10)
- Yaml de produccion (ver abajo ejemplo)
- Añadir tests (ver ejemplo abajo)
- REFACTOR: Renombrar paquete (sbit --> wearpipe)
- REFACTOR: Mover build_path function para eliminar io.py module (solo 1 funcion)
- REFACTOR: añade comilla invertidad en la formacion dinamica de SQL
- REFACTOR: eliminar io.py
- TEST: Schema drift: every deployed table must match the columns declared in thespecs/DDL — a renamed audit column fails here in one line, not as aSTREAM_FAILED in production
- CONFIG: flag to switch stream/batch modes (pocessing_times para microbatches??)

```yaml
tasks:
  - task_key: deploy_ddl
    python_wheel_task: {package_name: gym-pipeline, entry_point: deploy,
                        parameters: ["--env", "${bundle.target}"]}
  - task_key: bronze
    depends_on: [{task_key: deploy_ddl}]
    python_wheel_task: {package_name: gym-pipeline, entry_point: gym-pipeline,
                        parameters: ["--layer", "bronze", "--env", "${bundle.target}"]}
  - task_key: silver
    depends_on: [{task_key: bronze}]
    python_wheel_task: {package_name: gym-pipeline, entry_point: gym-pipeline,
                        parameters: ["--layer", "silver", "--env", "${bundle.target}"]}
  - task_key: gold
    depends_on: [{task_key: silver}]
    python_wheel_task: {package_name: gym-pipeline, entry_point: gym-pipeline,
                        parameters: ["--layer", "gold", "--env", "${bundle.target}"]}
```

--------------------------------------------------------------------------------------------------------------

## Qué es startingVersion
En el contexto de Databrikcs, startingVersion es el punto de arranque del stream cuando el checkpoint está vacio.

Una tabla Delta lleva un log de commits versionado (v0 = creación, v1, v2... cada append/update es una versión). Cuando haces readStream.table(), Spark debe decidir en qué punto del log arranca.

En el proyecto, starting_point controla desde qué versión del log e la tabla Delta empieza a leer el stream la primera vez que arranca (checkpoint vacio).

Casos de uso más comunes:

- Backfill. Si corriges un bug en una transformación silver y quieres regenerar las tablas: borras los checkpoints (y truncas los targets si quieres), relanzas con starting_version=0, y el stream relee todo el bronze. Los MERGE son idempotentes por clave, así que el reproceso converge al estado correcto sin duplicados — es la razón por la que los upserters del original están diseñados con MERGE y no con append ciego.

- Reproceso parcial tras un bug. En lugar de releer todo, consultas el punto del error (`DESCRIBE HISTORY dev.sbit_db.registered_users_bz`)
localizando la versión previa al commit problemático, borras el checkpoint del stream afectado y relanzas con --starting-version N: solo se reprocesa desde ahí.

- Recuperación operativa. Si un checkpoint se corrompe o se pierde, starting_version define de dónde re-sembrar el stream en vez de depender de las semánticas por defecto del snapshot.

- Cold-start explícito. Sin la opción, el comportamiento de primera arranque (snapshot completo) es implícito y poco conocido. Fijar 0 lo hace determinista y documentado.

**IMPORTANTE:** Sólo aplica con checkpoint vacío. Una vez que el stream tiene checkpoint, Spark ignora completamente staritngVersion. El stream siempre arranca desde su checkpoint, con independencia del valor starting_point que le pases al motor.

## Change Data Capture (CDC)
Nuestra capa Silver, en lugar de procesar sólo snapshot completos, se basa en la tabla Delta, que soporta CDC en streaming, para procesar todos los cambios (inserciones, actualizaciones y borrados) en origen (capa Bronze). Los streamings de Silver son CDC, y los streamings de Gold son CDC+MERGE.

A continuación se explican las opciones existentes del reader:

- startingVersion: qué versión del log leer cuando el checkpoint está vacío (backfill completo con 0, ventana con N).

- ignoreDeletes: tolera que se borren ficheros de la tabla fuente (p. ej. DELETE por partición) sin que el stream falle. Sin ella, cualquier delete en el origen mata el stream con una excepción, porque Structured Streaming asume fuentes append-only.

- ignoreChanges: tolera que la fuente sufra UPDATE/MERGE/OVERWRITE que reescriben ficheros. Matiz importante: no propaga el cambio correctamente — el stream re-emite las filas afectadas como si fueran nuevas. Por eso solo es seguro cuando el sink sabe deduplicar/actualizar: es exactamente el caso de user_bins (lee user_profile, que es SCD Type 1 y recibe MERGE ... UPDATE), donde el sink re-machea por user_id y actualiza.

- withEventTimeOrder: fuerza el procesamiento en orden de event-time cuando hay dedupe/watermark y la fuente recibe updates desordenados. Tu proyecto la dejó desactivada.

- skipChangeCommits (DBR moderno, alternativa): salta directamente los commits que contienen cambios/borrados, sin re-emitir nada. Semántica distinta de ignoreChanges (tolerar vs. descartar); útil si el sink no quiere ver filas re-escritas.

El proyecto también aplica CDC para ingerir los eventos de Kafka en la capa Bronze. En este caso, se procesan todos los eventos, sin importar si son nuevos o actualizados, basado en èvent_type` (new / insert / update).

## Data engines

The pipeline is built on a simple architecture: declarative specs (describe what moves through the pipeline), and two generic engines (define how data is moved).Adding a new table means adding a spec entry. No code changes are required in the engines:

- Bronze pipeline: BronzeIngester class (engine) / Input (raw data files) / Output (Delta tables)
- Gold and Silver pipelines: UpserterPipeline class (engine) / Input (Delta tables) / Output (Delta tables)

### BronzeIngester

The bronze engine lands raw files from the landing zone into Delta tables using Auto Loader (cloudFiles), which incrementally discovers newfiles and guarantees exactly-once ingestion via checkpointing.

Each source is declared as a SourceFeed spec in bronze_specs.py:

- target name, file format and explicit schema
- reader options (e.g. header, maxFilesPerTrigger to throttleprocessing on limited compute)
- a pure transform (e.g. adding audit columns) applied to the stream
- a scheduler pool tag

The engine builds one streaming query per feed:

landing_zone/<name>  →  readStream(cloudFiles)  →  transform  →  writeStream  →  toTable (<name>, append)

Key properties:

- Append-only: bronze never updates or deletes; the target table is the immutable landing of raw data.
- Schema-first: the schema is declared in the spec, not inferred,so ingestion never fails on a drifted file.
- Idempotent replay: checkpoint state makes availableNow runsrepeatable — re-running drains only new files.


### UpserterPipeline

The silver and gold layers share a single engine that reads Delta tables as change streams and applies each micro-batch with a MERGE (via `DeltaTable.merge` inside 'foreachBatch`), making targets idempotent by business key.

Each target is declared as a StreamSpec in silver_specs.py / gold_specs.py. For the standard single-source path the engine handles:

- CDC reader options: `startingVersion` and `ignoreDeletes` / `ignoreChanges` (tolerating deletes on bronze or SCD1 updates on silver sources without killing the stream)
- Event-time processing: watermark before `dropDuplicates`, so deduplication state is cleaned up automatically
- Per-batch logic: `batch_transform` runs inside `foreachBatchfor` operations not allowed on streaming DataFrames (e.g. CDCrank() windowing to keep the latest event per key)
- The MERGE itself is declarative (MergeSpec): a join condition plusoptional guarded UPDATE and INSERT clauses. The upsert logic in oneplace, applied identically to every target.

Targets that escape the single-source pattern (stream-stream joins such as `completed_workouts`, multi-source joins with static dimensions such as `workout_bpm`, streaming aggregations such as `workout_bpm_summary`) use a custom builder function (`build_df`) that constructs the streaming DataFrame and hands it back to the engine.

Dependent targets are executed in explicit phases (PHASES / GOLD_PHASES) with the `run` method. All streams  in a phase run concurrently and complete before the next pahse starts, so a target never consumes a table that has not been fully ingested yet.

### MERGE safety

All silver and gold writes go through DeltaTable.MERGE applied to the current micro-batch. Safety does not come from a single mechanism but from independent layers, each covering a distinct failure mode.

- Idempotency by business key. Every MERGE matches on the target's natural key (user_id, device_id + time, etc). Replayed batches, backfills re-run with `--sarting-version` and cleared checkpoint, and rows re-emitted by source rewrites all converge to the same state: the MERGE updates the matched row or skips it (never duplicates it). This makes processing cheap and safe.
- Update policy per target. Update semantics are a modeling decision, declared in each spec's MergeSpec (insert-only, last-writer-wins, slowly-changing-dimension overwrite, monotonic advance)
- Deduplication. Stream level (`dropDuplicates`) applied after the event-time watermark bounds the dedup state and cleans it up automatically (checkpoint-owned).
Batch level (rank per key, keep latest) runs inside `foreachBatch`, where window functions are allowed on micro-batches. Events arriving later than the watermark are dropped by design. The accepted trade-off for bounded streaming state.
- Bounded merge scope. The MERGE source is always the current micro-batch, never a full table. Merge cost scales with batch size, not with target size.

## Pipeline preconditions

- Targets pre-exist: DDL is deployed before any stream starts(deploy_ddl task); DeltaTable.forName fails fast otherwise.
- Phase ordering: a dependent target starts only after its inputs arefully drained (PHASES, awaited per phase).
- Append-only bronze: silver MERGEs assume bronze never updates.ignoreDeletes / ignoreChanges tolerate source rewrites, and anyrows re-emitted by them are reconciled by the idempotent MERGE.


## Running

Both engines support two trigger modes, selected by --stream:

- Default (availableNow): drains all pending data and exits —batch-style, cost-efficient, idempotent. Use for backfills andscheduled jobs.
- --stream (continuous): triggers every --processing-timeinterval and runs indefinitely. Use for low-latency serving.


