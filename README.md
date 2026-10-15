
WEARABLE DATA PROCESSOR OVER DATALAKEHOUSE: wearpipe

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
```python
# src/etl_ventas/transforms.py
from pyspark.sql import DataFrame


def dedupe(df: DataFrame, keys: list[str]) -> DataFrame:
    return df.dropDuplicates(keys)


def normalize_amounts(df: DataFrame, col: str) -> DataFrame:
    from pyspark.sql import functions as F

    return df.withColumn(col, F.round(F.col(col).cast('decimal(18,2)'), 2))
```

Sample entry point invoked by job in prod. The code does not know anything about the runtime (dev or prod).
The environment is read from the command line (argument):
```python
# src/etl_ventas/jobs.py
import argparse

from etl_ventas.runtime import get_spark
from etl_ventas import transform


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)  # cambia por entorno
    parser.add_argument('--table', required=True)
    args = parser.parse_args()

    spark = get_spark()  # ← en prod: sesión nativa; en local: remota

    source = spark.read.table(f'{args.catalog}.bronze.{args.table}')
    clean = normalize(transform.dedupe(source, keys=['id']))

    clean.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{args.catalog}.silver.{args.table}')
```
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
- Bronze lee archivos, escribe directamente sin MERGE (append)
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

```python
# Pattern for dual sessions: dev/CI/CD on databricks-connect | prod on DBR
# src/etl_ventas/runtime.py
"""Fábrica de SparkSession dual.

- En Databricks (job/notebook): la sesión nativa del cluster.
- En local/CI (con databricks-connect): sesión remota por gRPC.
"""

import os


def get_spark():
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        # Ejecutándose DENTRO de Databricks (job, notebook, pipeline)
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()

    # Ejecutándose en local o CI
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.remote().getOrCreate()
    # serverless: DatabricksSession.builder.remote(serverless=True).getOrCreate()
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

Sample tests:
```python
# tests/integration/conftest.py
import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope='session')
def spark():
    return DatabricksSession.builder.remote().getOrCreate()


# tests/integration/test_pipeline.py
from etl_ventas.transform import dedupe


def test_dedupe_contra_datos_reales(spark):
    df = spark.table('dev.bronze.customers').limit(1000)
    result = dedupe(df, keys=['id'])
    assert result.count() <= df.count()


# tests/unit/test_transform.py
from chispa import assert_df_equality
from etl_ventas.transform import dedupe


def test_dedupe(spark_local):  # fixture con pyspark local (sin cluster)
    df = spark_local.createDataFrame([(1, 'a'), (1, 'a'), (2, 'b')], 'id int, v string')
    assert_df_equality(dedupe(df, ['id']), spark_local.createDataFrame([(1, 'a'), (2, 'b')], 'id int, v string'))
```
Piramide recomendada: lo mas rapido y numeroso son los test unitarios con Spark local

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


Si se desea montar tests unitarios locales con Spark local se recomienda python-slim + pyspark

DECISION:
14/10: para proyecto de aprendizaje mantenemos el aprovisionamiento de recursos via SQL DDL. Se deja como mejora la integración con herramientas IaC
como Terraform o Pulumi. Sin embargo, queda pendiente la refactorizacion para implementar el layout profesional. En este caso, debemos sacar toda la logica DDL de la aplicacion para que se invoque coo un paso mas del CI antes del bundle deploy. Para ello debemos mover todo el codigo DDL a un modulo bootstrap.py en la carpeta raiz. También creamos un nuevo modulo preconditions.py dentro de "lib" para las validaciones (valida los recursos de infra necesarios para el proyecto).

DECISION TESTING: para la version inicial se usa databricks.connect para el juego de tests, sin embargo el fixture ppal en conftest.py incluye el uso de una variable de entorno para habilitar el uso de un backend local sin cambiar el código del juego de tests:
```python
import os


@pytest.fixture(scope='session')
def spark():
    if os.getenv('SPARK_TEST_BACKEND', 'remote') == 'local':
        from pyspark.sql import SparkSession

        return SparkSession.builder.master('local[2]').getOrCreate()

    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.remote(serverless=True).getOrCreate()
```

Configuracion: envars + cli args (sobreescribe envars)

MEJORA: sustituir print por logging

OBJETIVO: 0 dependencias externas (solo libreria standard de Python) ==> workers mas ligeros (sin interprete Python)

SPARK CONFIG:

* **`spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)`**
Ajusta el número de particiones tras un *shuffle* (como en *joins* o agregaciones) para que coincida con el nivel de paralelismo por defecto del clúster (`sc.defaultParallelism`). Evita usar el valor por defecto de Spark (200), que suele ser ineficiente para datasets pequeños o medianos.
* **`spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)`**
Activa la escritura optimizada en tablas Delta. Agrupa y reorganiza las particiones de forma dinámica antes de escribir en disco para evitar generar miles de archivos pequeños (*small files problem*).
* **`spark.databricks.delta.autoCompact.enabled`**
Habilita la compactación automática en Delta Lake. Tras una escritura, comprueba si hay archivos pequeños y los fusiona en archivos más grandes de manera automática, reduciendo la necesidad de ejecutar comandos `OPTIMIZE` manuales.
* **`spark.sql.streaming.stateStore.providerClass`**
Configura **RocksDB** como el proveedor del almacén de estado para *Spark Structured Streaming*. Es ideal para flujos con mucho estado (como *joins* o agregaciones en streaming), ya que gestiona los datos fuera de la memoria JVM principal, evitando errores de falta de memoria (*OOM*).

