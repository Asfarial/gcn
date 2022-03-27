"""
Reader Module
"""
import csv
import gzip
from tabulate import tabulate
from app.core import FileSystemBase, Verbose
from app.config import cmd_args


class ReaderBase(FileSystemBase, Verbose):
    """
    Reader Low-Level Interface
    """
    def __init__(self, *args, **kwargs) -> None:
        Verbose.__init__(self, kwargs.pop("verbose", None))
        FileSystemBase.__init__(self, *args, **kwargs)
        self._message("Initializing File System checks")
        self._folder_exists()
        self._files_exist()
        self._message("File system checks - OK")

    @staticmethod
    def discount(line: dict) -> str:
        """
        Calculating discount
        :param line:
        :return:
        """
        def is_number(number: str) -> bool:
            return number.replace(".", "", 1).isdigit()

        price = str(line["price"])
        old_price = str(line["old_price"])
        if not is_number(price) or not is_number(old_price):
            return ""

        price = float(price)
        old_price = float(old_price)
        if price >= old_price:
            return ""

        return f"{100 - price * 100 / old_price:.0f}%"

    @staticmethod
    def shrink_line(line: dict, length: int = 64) -> dict:
        """
        Shrinks particular line
        called from self.shrink_list
        :param line:
        :param length:
        :return:
        """
        return {
            key: value[:length] + "..." if len(value) > length else value
            for key, value in line.items()
        }

    @staticmethod
    def shrink_list(records_list: list, length: int = 64) -> list:
        """
        Shrinks list of records from Reader.print
        :param records_list:
        :param length:
        :return:
        """
        return [Reader.shrink_line(item, length) for item in records_list]


class Reader(ReaderBase):
    """
    Reader High-Level Interface
    """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._message("Initializing Reader")

    def read_eans(self) -> list:
        """
        Parse active Eans
        :return:
        """
        self._message("Reading Eans")
        with open(self.files["eans"], "r", encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            return [line["ean"] for line in reader if line["active"] == "1"]

    def read_data(self, eans: list = None) -> list:
        """
        Parse and filter data from csv
        :param eans:
        :return:
        """
        self._message(
            f"Reading Data. "
            f"Filter: {'eans - active only' if eans else 'all records'}"
        )
        with gzip.open(self.files["data"], "rt") as csvfile:
            reader = csv.DictReader(csvfile)
            if eans:
                return [
                    line | {"discount": self.discount(line)}
                    for line in reader
                    if line["ean"] in eans
                ]
            return [
                line | {"discount": self.discount(line)} for line in reader
            ]

    @staticmethod
    def print_out(
        data: list[dict[str:str]], shrink: bool = cmd_args.shrink
    ) -> None:
        """
        Print out parsed and filtered data from csv file
        :param data:
        :param shrink:
        :return:
        """
        data = Reader.shrink_list(data) if shrink else data
        print(
            tabulate(
                data,
                headers="keys",
                showindex=True,
                tablefmt="psql",
                numalign="left",
                stralign="left",
            )
        )
