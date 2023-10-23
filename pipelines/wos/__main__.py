import yaml
from pathlib import Path
from lib import TaskRunnerCli

from .pipeline import WosConfig, create_tasks

cli = TaskRunnerCli()
cli.add_argument("-c", "--config", type=Path, required=True, help="YAML config file")
action, args = cli.parse_args()

with args.config.open() as config_file:
    config = WosConfig.model_validate(yaml.safe_load(config_file)["source"])

action(create_tasks(config))
