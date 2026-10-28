"""Setup helpers.

Bronze tables are created from DDL strings. Their schemas are dictated by
the raw landing format (CSV/JSON DDL strings in bronze_specs), not by a transform contract.

Silver/Gold tables are created from the specs in their respective modules' target_schema.

Each target_schema is converted to backtick-quoted DDL via ddl_for_spec(). This is the single
source of truth for silver/gold schemas: changing a spec's target_schema automatically updates the
physical table definition on the next setup_db() call.

SQL views are created separately because its definition involves a multi-table join that
does not fit the spec pattern.
"""

import sys
import time
from pathlib import Path
from typing import Any

from databricks.connect.session import DatabricksSession
from databricks.sdk import WorkspaceClient

from wearpipe.config import load_settings
from wearpipe.ddl import ddl_for_spec
from wearpipe.gold_specs import GOLD_STREAMS
from wearpipe.history_loader import load_history
from wearpipe.io import build_object_path
from wearpipe.silver_specs import SILVER_STREAMS


class SetupHelper:
    def __init__(self, spark: Any, catalogue_name: str, db_name: str, volume_name: str, data_root: str) -> None:
        self.spark = spark
        self.catalogue: str = catalogue_name
        self.db_name: str = db_name
        self.initialized: bool = False
        self.volume_name = volume_name
        self.volume_exists: bool = False
        self.data_root = data_root

    def create_db(self) -> None:
        print(f'Creating the database {self.catalogue}.{self.db_name}... ', end='')
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.catalogue}.{self.db_name}')
        self.spark.sql(f'USE {self.catalogue}.{self.db_name}')
        self.initialized = True
        print('Done!')

    def create_users_bronze_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating registered_users_bz table... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalogue}.{self.db_name}.registered_users_bz(
                user_id LONG,
                device_id LONG,
                mac_address STRING,
                registration_timestamp DOUBLE,
                load_time TIMESTAMP,
                source_file STRING
            )"""
        )
        print('Done!')

    def create_gym_logins_bronze_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating gym_logins_bz table... ', end='')
        self.spark.sql(
            f"""
            CREATE OR REPLACE TABLE {self.catalogue}.{self.db_name}.gym_logins_bz (
                mac_address STRING,
                gym BIGINT,
                login DOUBLE,
                logout DOUBLE,
                load_time TIMESTAMP,
                source_file STRING
            )"""
        )
        print('Done!')

    def create_kafka_multiplex_bronze_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating kafka_multiplex_bz table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalogue}.{self.db_name}.kafka_multiplex_bz(
                key STRING,
                value STRING,
                partition BIGINT,
                offset BIGINT,
                timestamp BIGINT,
                date DATE,
                load_time TIMESTAMP,
                source_file STRING,
                topic STRING,
                week_part STRING)
                PARTITIONED BY (topic, week_part)
            """
        )
        print('Done!')

    def create_silver_tables(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating silver tables from specs... ', end='')
        for spec in SILVER_STREAMS.values():
            ddl = ddl_for_spec(spec, self.catalogue, self.db_name)
            self.spark.sql(ddl)
        print('Done!')

    def create_gold_tables(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating gold tables from specs... ', end='')
        for spec in GOLD_STREAMS.values():
            ddl = ddl_for_spec(spec, self.catalogue, self.db_name)
            self.spark.sql(ddl)
        print('Done!')

    def create_date_lookup_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating date_lookup table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalogue}.{self.db_name}.date_lookup(
                date DATE,
                week INT,
                month INT,
                year INT,
                dayofweek INT,
                dayofmonth INT,
                dayofyear INT,
                week_part STRING
            )"""
        )
        print('Done!')

    def create_workout_bpm_summary_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating workout_bpm_summary table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalogue}.{self.db_name}.workout_bpm_summary(
                workout_id INT,
                session_id INT,
                user_id BIGINT,
                age STRING,
                gender STRING,
                city STRING,
                state STRING,
                min_bpm DOUBLE,
                avg_bpm DOUBLE,
                max_bpm DOUBLE,
                num_recordings BIGINT
            )"""
        )
        print('Done!')

    def create_gym_summary_view(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating gym_summary view ... ', end='')
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.catalogue}.{self.db_name}.gym_summary AS
                SELECT to_date(login::timestamp) date,
                gym, l.mac_address, workout_id, session_id,
                round((logout::long - login::long)/60,2) minutes_in_gym,
                round((end_time::long - start_time::long)/60,2) minutes_exercising
                FROM gym_logs l
                JOIN (
                SELECT mac_address, workout_id, session_id, start_time, end_time
                FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                ON l.mac_address = w.mac_address
                AND w. start_time BETWEEN l.login AND l.logout
                order by date, gym, l.mac_address, session_id
            """
        )
        print('Done!')

    def setup_db(self) -> None:
        print('Starting database setup...')
        start_time = int(time.time())

        self.create_db()
        self.create_users_bronze_table()
        self.create_gym_logins_bronze_table()
        self.create_kafka_multiplex_bronze_table()
        self.create_silver_tables()
        self.create_gold_tables()
        self.create_date_lookup_table()
        self.create_workout_bpm_summary_table()
        self.create_gym_summary_view()

        print(f'DB setup completed in {int(time.time()) - start_time} seconds.')

    def cleanup_db(self) -> None:
        if self.spark.catalog.databaseExists(f'{self.catalogue}.{self.db_name}'):
            print(f'Dropping the database {self.catalogue}.{self.db_name} ... ', end='')
            self.spark.sql(f'DROP DATABASE IF EXISTS {self.catalogue}.{self.db_name} CASCADE')
            print('Done!')

    def setup_volume(self) -> None:
        print(f'Creating volume {self.volume_name} ... ', end='')

        full_name = f'`{self.catalogue}`.`{self.db_name}`.`{self.volume_name}`'
        self.spark.sql(f'CREATE VOLUME IF NOT EXISTS {full_name}')
        result = self.spark.sql(
            f'SELECT 1 FROM `{self.catalogue}`.information_schema.volumes '
            f"WHERE volume_name = '{self.volume_name}' "
            f"  AND volume_schema = '{self.db_name}' "
            f"  AND volume_catalog = '{self.catalogue}'"
        ).collect()

        if not result:
            self.is_volume_created = False
            raise RuntimeError(f'Volume {full_name} not found in information_schema after CREATE')
        self.volume_exists = True

        print('Done!')

    def cleanup_volume(self) -> None:
        print(f'Dropping the volume {self.catalogue}.{self.db_name}.{self.volume_name} ... ', end='')
        self.spark.sql(f'DROP VOLUME IF EXISTS `{self.catalogue}`.`{self.db_name}`.`{self.volume_name}`')
        print('Done!')

    def upload_date_lookup_file(self) -> str:
        remote_path = build_object_path(
            self.catalogue,
            self.db_name,
            self.volume_name,
            self.data_root,
            'tests',
            'date-lookup.json',
        )
        local_path = Path(__file__).parent.parent.parent / 'data' / 'date-lookup.json'

        w = WorkspaceClient()
        w.files.upload_from(remote_path, str(local_path), overwrite=True)

        return remote_path


def main() -> None:
    try:
        config = load_settings()

        spark = DatabricksSession.builder.getOrCreate()

        # SETUP DATABASE
        setup_manager = SetupHelper(
            spark, config.catalogue_name, config.db_name, config.volume_name, config.base_data_path
        )
        setup_manager.setup_db()

        # SETUP VOLUME
        setup_manager.setup_volume()

        # UPLOAD HISTORY FILES
        if setup_manager.is_volume_created:
            setup_manager.upload_date_lookup_file()

        # LOAD HISTORY DATA
        file_path = build_object_path(
            config.catalogue_name,
            config.db_name,
            config.volume_name,
            config.base_data_path,
            'test_data',
            'date-lookup.json',
        )
        load_history(spark, file_path)

    except Exception as e:
        print(f'Initial setup failed with the following error: {e}')
        sys.exit(1)
