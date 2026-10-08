import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    base_data_path: str = os.environ['BASE_DATA_PATH']
    base_checkpoint_path: str = os.environ['BASE_CHECKPOINT_PATH']
    db_name: str = os.environ['DB_NAME']
    max_files_per_trigger: int = 1000


# Global Config instance
config = Config()
