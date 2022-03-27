"""
Configuration Module
"""
import os
import argparse


def arg_parser() -> argparse.Namespace:
    """
    Bash
    Arguments parser
    :return:
    """
    parser = argparse.ArgumentParser(description="GCN Data Pipeline")
    parser.add_argument(
        "-v",
        "--verbose",
        help="Verbose output",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-d", "--debug", help="Debug run", required=False, action="store_true"
    )
    parser.add_argument(
        "-sh",
        "--shrink",
        help="Shrink output tables",
        required=False,
        action="store_true",
    )
    return parser.parse_args()


cmd_args = arg_parser()

DEBUG = cmd_args.debug if cmd_args.debug else os.environ.get("DEBUG", False)

FOLDER = "./records"
EANS = "eans.csv"
DATA = "product_data_0.csv.gz"

DB_USER = os.environ.get("DB_USER", "test")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "test")
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DATABASE = os.environ.get("DATABASE", "test")
