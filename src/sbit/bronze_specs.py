from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from sbit.transforms import add_audit_columns, join_date_lookup


@dataclass(frozen=True)
class SourceFeed:
    name: str
    file_format: str
    schema: str
    options: dict[str, str] = field(default_factory=dict)
    transform: Callable[[DataFrame], DataFrame] | None = field(default=None)


FEEDS = [
    SourceFeed(
        name='registered_users_bz',
        file_format='csv',
        schema='user_id long, device_id long, mac_address string, registration_timestamp double',
        transform=add_audit_columns,
        options={'header': 'true', 'maxFilesperTrigger': '1'},
    ),
    SourceFeed(
        name='gym_logins_bz',
        file_format='csv',
        schema='mac_address string, gym bigint, login double, logout double',
        options={'header': 'true', 'maxFilesPerTrigger': '1'},
        transform=add_audit_columns,
    ),
    SourceFeed(
        name='kafka_multiplex_bz',
        file_format='json',
        schema='key string, value string, topic string, partition bigint, offset bigint, timestamp bigint',
        options={'maxFilesPerTrigger': '1'},
        transform=lambda df: join_date_lookup(add_audit_columns(df)),
    ),
]
