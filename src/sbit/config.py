import os
from dataclasses import dataclass
from pathlib import Path


def _load_env(filepath: str | Path = '.env') -> None:
    """Load environment variables from a .env file into os.environ."""
    env_path = Path(filepath)
    if not env_path.is_file():
        return

    for line in env_path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue

        key, value = line.split('=', 1)
        key, value = key.strip(), value.strip()

        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        os.environ.setdefault(key, value)


@dataclass
class Config:
    catalogue_name: str = os.getenv('CATALOGUE_NAME', '')
    db_name: str = os.getenv('DB_NAME', '')
    volume_name: str = os.getenv('VOLUME_NAME', 'data_zone')
    max_files_per_trigger: int = 1000
    base_data_path: str = os.getenv('BASE_DATA_PATH', 'raw')
    base_checkpoint_path: str = os.getenv('TEST_DATA_PATH', 'chkpnt')


_load_env()

# Global Config instance
config = Config()
