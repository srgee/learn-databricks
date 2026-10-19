import os
from argparse import ArgumentParser
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, get_type_hints


@dataclass(frozen=True)
class Config:
    """Single source of truth for config settings."""

    catalogue_name: str = field(
        default='dev',
        metadata={
            'flag': '--catalogue',
            'env_var': 'WEARPIPE_CATALOGUE',
            'help': 'Databricks catalogue name (Unity Catalogue)',
        },
    )
    db_name: str = field(
        default='db',
        metadata={
            'flag': '--db-name',
            'env_var': 'WEARPIPE_DB_NAME',
            'help': 'Databricks database name (schema)',
        },
    )
    volume_name: str = field(
        default='vol_name',
        metadata={
            'flag': '--volume',
            'env_var': 'WEARPIPE_VOLUME',
            'help': 'Databricks managed volume name',
        },
    )
    base_data_path: str = field(
        default='raw',
        metadata={
            'flag': '--base-data-path',
            'env_var': 'WEARPIPE_BASE_DATA_PATH',
            'help': 'Root data directory in Databricks managed volume',
        },
    )
    base_checkpoint_path: str = field(
        default='chkpnt',
        metadata={
            'flag': '--base-checkpoint-path',
            'env_var': 'WEARPIPE_BASE_CHECKPOINT_PATH',
            'help': 'Root checkpoint directory in Databricks managed volume',
        },
    )
    max_files_per_trigger: int = field(
        default=1000,
        metadata={
            'flag': '--max-files-per-trigger',
            'help': 'Maximum number of files to process per trigger in Spark Structured Streaming to limit input flow to avoid OOM errors',  # noqa: E501
        },
    )
    stream_flag: bool = field(
        default=False,
        metadata={
            'flag': '--stream',
            'is_flag': True,
            'action': 'store_true',
            'help': 'Enable Spark Structured Streaming',
        },
    )
    layer: str = field(
        default='bronze',
        metadata={
            'flag': '--layer',
            'env': 'WEARPIPE_LAYER',
            'choices': ['bronze', 'silver', 'gold', 'all'],
            'help': 'Pipeline layer to run',
        },
    )
    processing_time: str = field(
        default='15 seconds',
        metadata={
            'flag': '--processing-time',
            'env': 'WEARPIPE_PROCESSING_TIME',
            'help': 'Trigger interval for continuous streaming mode',
        },
    )
    starting_version: int = field(
        default=0,
        metadata={
            'flag': '--starting-version',
            'env': 'WEARPIPE_STARTING_VERSION',
            'help': 'Delta version to start from; only honored when the stream checkpoint is empty',
        },
    )

    def __post_init__(self) -> None:
        """Validate arguments' choices."""
        for f in fields(self):
            allowed = f.metadata.get('choices')
            if allowed is not None and getattr(self, f.name) not in allowed:
                raise ValueError(f'{f.name}={getattr(self, f.name)!r}; valores válidos: {allowed}')


def _load_env_file(filepath: str | Path = '.env') -> None:
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


def _cast(value: str, type_: type[Any]) -> Any:
    """Convert a string value to the specified data type.

    Args:
        value: The string value to convert.
        type_: The target data type (e.g., int, float, bool, str).

    Returns:
        The converted value.
    """
    if type_ is bool:
        return value.strip().lower() in {'1', 'true', 'yes', 'on'}
    return type_(value)  # str, int, float


def build_parser() -> ArgumentParser:
    """Generate the CLI parser from Config attributes.

    Every attribute contributes exactly one option. Field metadata drives
    the shape: 'is_flag' -> store_true, 'choices' -> argparse-level
    validation, 'help' -> help text. Fields without a metadata key get
    None, which argparse treats as "not applicable".
    """
    parser = ArgumentParser()
    for f in fields(Config):
        if f.metadata.get('is_flag'):
            parser.add_argument(
                f.metadata['flag'], dest=f.name, action='store_true', default=None, help=f.metadata.get('help')
            )
        else:
            parser.add_argument(
                f.metadata['flag'],
                dest=f.name,
                default=None,
                choices=f.metadata.get('choices'),
                help=f.metadata.get('help'),
            )
    return parser


def load_settings(argv: list[str] | None = None, env_file: str | Path | None = '.env') -> Config:
    """Settings priority: CLI > env > default values.

    Args:
        argv: List of command-line arguments. If None, sys.argv[1:] is used.
        env_file: Path to .env file. If None, .env load is skipped.

    Returns:
        Config instance
    """
    if env_file is not None and Path(env_file).is_file():
        _load_env_file(Path(env_file))

    ns = build_parser().parse_args(argv)
    types = get_type_hints(Config)
    kwargs = {}

    for f in fields(Config):
        cli_value = getattr(ns, f.name)  # None if it did not come from CLI
        env_value = os.getenv(f.metadata.get('env_var', 'NO_ENV_VAR'))  # None if it does not exit in the environment
        if cli_value is not None:
            kwargs[f.name] = _cast(cli_value, types[f.name]) if isinstance(cli_value, str) else cli_value
        elif env_value is not None:
            kwargs[f.name] = _cast(env_value, types[f.name])
        else:
            kwargs[f.name] = f.default
    return Config(**kwargs)
