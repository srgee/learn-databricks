import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    catalogue_name: str = os.getenv('CATALOGUE_NAME', '')
    db_name: str = os.getenv('DB_NAME', '')
    volume_name: str = os.getenv('VOLUME_NAME', 'data')
    max_files_per_trigger: int = 1000
    base_data_path: str = os.getenv('BASE_DATA_PATH', 'raw')
    test_data_path: str = os.getenv('TEST_DATA_PATH', 'test')


# Global Config instance
config = Config()
