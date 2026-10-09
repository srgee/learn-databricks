import time
from pathlib import Path
from typing import Any

from sbit.config import config


class SetupHelper:
    def __init__(self, env: str, spark: Any) -> None:
        self.env = env
        self.spark = spark
        self.landing_zone: Path = Path(config.base_data_path) / 'raw'
        self.checkpoint_path: Path = Path(config.base_checkpoint_path) / 'checkpoints'
        self.catalog: str = env
        self.db_name: str = config.db_name
        self.initialized: bool = False
        self.tables: list[str] = [
            'registered_users_bz',
            'gym_logins_bz',
            'kafka_multiplex_bz',
            'users',
            'gym_logs',
            'user_profile',
            'heart_rate',
            'user_bins',
            'workout_bpm',
            'workouts',
            'completed_workouts',
            'date_lookup',
            'gym_summary',
            'workout_bpm_summary',
        ]

    def create_db(self) -> None:
        print(f'Creating the database {self.catalog}.{self.db_name}... ', end='')
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}')
        self.spark.sql(f'USE {self.catalog}.{self.db_name}')
        self.initialized = True
        print('Done!')

    def create_users_bronze_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating registered_users_bz table... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
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
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz (
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
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
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

    def create_users_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating users table... ', end='')
        self.spark.sql(
            f"""
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                user_id BIGINT,
                device_id BIGINT,
                mac_address STRING,
                registration_timestamp TIMESTAMP
            )"""
        )
        print('Done!')

    def create_gym_logs_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating gym_logs table ... ', end='')
        self.spark.sql(
            f"""
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                mac_address STRING,
                gym BIGINT,
                login TIMESTAMP,
                logout TIMESTAMP
            )"""
        )
        print('Done!')

    def create_user_profile_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating user_profile table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                user_id BIGINT,
                dob DATE,
                gender STRING,
                first_name STRING,
                last_name STRING,
                street_address STRING,
                city STRING,
                state STRING,
                zip INT,
                updated TIMESTAMP
            )"""
        )
        print('Done!')

    def create_heart_rate_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating heart_rate table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                device_id LONG,
                heart_rate DOUBLE,
                valid BOOLEAN
            )"""
        )
        print('Done!')

    def create_user_bins_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating user_bins table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                user_id BIGINT,
                age STRING,
                gender STRING,
                city STRING,
                state STRING
            )"""
        )
        print('Done!')

    def create_workouts_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating workouts table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                user_id INT,
                workout_id INT,
                time TIMESTAMP,
                action STRING,
                session_id INT
            )"""
        )
        print('Done!')

    def create_completed_workouts_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating completed_workouts table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP
            )"""
        )
        print('Done!')

    def create_workout_bpm_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating workout_bpm table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                time TIMESTAMP,
                heart_rate DOUBLE
            )"""
        )
        print('Done!')

    def create_date_lookup_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating date_lookup table ... ', end='')
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
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
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
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

    def create_gym_summary_table(self) -> None:
        if not self.initialized:
            raise KeyError('Missing configuration settings')
        print('Creating gym_summary table ... ', end='')
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
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

    def setup(self) -> None:
        print('Starting setup...')
        start_time = int(time.time())

        self.create_db()
        self.create_users_bronze_table()
        self.create_gym_logins_bronze_table()
        self.create_kafka_multiplex_bronze_table()
        self.create_users_table()
        self.create_gym_logs_table()
        self.create_user_profile_table()
        self.create_heart_rate_table()
        self.create_workouts_table()
        self.create_completed_workouts_table()
        self.create_workout_bpm_table()
        self.create_user_bins_table()
        self.create_date_lookup_table()
        self.create_workout_bpm_summary_table()
        self.create_gym_summary_table()

        print(f'Setup completed in {int(time.time()) - start_time} seconds.')

    def assert_table(self, table_name: str) -> bool:
        if self.spark.catalog.tableExists(f'{self.catalog}.{self.db_name}.{table_name}'):
            print(f'Found {table_name} in {self.catalog}.{self.db_name}. Success!')
            return True
        return False

    def validate(self) -> None:
        print('\n Starting setup validation ...')
        start_time = int(time.time())

        assert self.spark.catalog.databaseExists(f'{self.catalog}.{self.db_name}')
        count = 0
        for t in self.tables:
            if not self.assert_table(t):
                print(f'ERROR: table {t} does not exist in {self.catalog}.{self.db_name}')
                count += 1

        print(f'Setup validation completed in {int(time.time()) - start_time} seconds with {count} errors.')

    def cleanup(self) -> None:
        if self.spark.catalog.databaseExists(f'{self.catalog}.{self.db_name}'):
            print(f'Dropping the database {self.catalog}.{self.db_name} ... ', end='')
            self.spark.sql(f'DROP DATABASE IF EXISTS {self.catalog}.{self.db_name} CASCADE')
            print('Done!')
