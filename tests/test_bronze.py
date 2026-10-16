import pytest

SETS = 2

CASES = [
    ('reg_users_bz', 5 if SETS == 1 else 10, None),
    ('gym_logins_bz', 8 if SETS == 1 else 16, None),
    ('kafka_multiplex_bz', 7 if SETS == 1 else 13, "topic = 'user_info'"),
    ('kafka_multiplex_bz', 16 if SETS == 1 else 32, "topic = 'workout'"),
    ('kafka_multiplex_bz', SETS * 253801, "topic = 'bpm'"),
]


@pytest.mark.parametrize('table,expected,where', CASES)
def test_bronze_counts(spark, table, expected, where):
    df = spark.read.table(f'dev.sbit_db.{table}')
    if where:
        df = df.where(where)
    assert df.count() == expected, f'Fallo en {table} where {where}'
