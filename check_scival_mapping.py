from argparse import ArgumentParser
from pathlib import Path


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("config", type=Path, help="Existing config path")
