from .scival import ScivalConfig
from .wos import WosConfig
from .pure import PureConfig

from .csv_utils import increase_field_size_limit as _increase_field_size_limit


_increase_field_size_limit()
SourceConfig = ScivalConfig | WosConfig | PureConfig
