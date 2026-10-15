"""
Capa de acceso a los datos: sabe cómo se leen y escriben los datos. Los pipeline solo expresan el qué.
Organizado por categorías, en orden de aparición en el proyecto:
# 1. Constructores de nombres/paths — puros, sin I/O, trivialmente testeables
def build_volume_path(catalog: str, schema: str, volume: str, *parts: str) -> str
def table_fqn(catalog: str, schema: str, table: str) -> str          # → "dev.sbit_db.customers"

# 2. Lecturas — envuelven spark.read con convenciones centralizadas
def read_json(spark, path: str) -> DataFrame
def read_delta(spark, fqn: str) -> DataFrame

# 3. Escrituras — el punto único donde se fija mode y opciones
def write_delta(df, fqn: str, mode: str = "overwrite") -> None

# 4. Disciplina de capas — combina 1+2+3 imponiendo la regla medallion en código
def read_bronze(spark, catalog: str, table: str) -> DataFrame
def write_silver(df, catalog: str, table: str) -> None
def write_gold(df, catalog: str, table: str) -> None

Añade nuevas funciones cuando se require (se repite por tercera vez, no antes)
"""


def build_object_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    return '/Volumes/' + '/'.join((catalog, schema, volume, *parts))
