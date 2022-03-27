"""
CI-like module
to check the code
"""
import os
import argparse


def arg_parser() -> argparse.Namespace:
    """
    Bash
    Arguments parser
    :return:
    """
    parser = argparse.ArgumentParser(
        description="Check and Format Project`s Code"
    )
    parser.add_argument(
        "-b",
        "--black",
        help="Black formatting",
        required=False,
        action="store_true",
    )
    return parser.parse_args()


BLACK = "black --exclude venv --line-length 79 ./*.py"

COMMANDS = (
    "pylint ./* --ignore=venv,"
    "Dockerfile,"
    "TO_DO,"
    ".idea,"
    ".git,"
    "records,"
    "*.txt,"
    "requirements.txt,"
    "dev-requirements.txt",

    "pycodestyle . --exclude=venv,TO_DO,.idea,.git,records",

    "python -m unittest"

)

args = arg_parser()

if args.black:
    os.system(BLACK)
for command in COMMANDS:
    print(command)
    os.system(command)
