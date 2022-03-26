import os
import csv
import gzip
from tabulate import tabulate

FOLDER = './records'
EANS = 'eans.csv'
DATA = 'product_data_0.csv.gz'


class ReaderBase:
    _folder = FOLDER
    _eans = EANS
    _data = DATA

    def __init__(
            self,
            folder: str = _folder,
            eans: str = _eans,
            data: str = _data,
    ):
        self._folder = self.normalize(folder)
        self._eans = self.normalize(eans)
        self._data = self.normalize(data)
        self._files = {key: self._make_path(path) for key, path in {'eans': self.eans, 'data': self.data}.items()}
        self._folder_exists()
        self._files_exist()

    @staticmethod
    def normalize(string: str):
        return string.strip().lower()

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def eans(self) -> str:
        return self._eans

    @property
    def data(self) -> str:
        return self._data

    @property
    def files(self) -> dict:
        return self._files

    def _make_path(self, file: str) -> str:
        return os.path.join(self.folder, file)

    def _folder_exists(self) -> bool:
        error_msg = f"Folder: {self.folder}"
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
            raise FileNotFoundError(error_msg)
        if not os.path.isdir(self.folder):
            raise NotADirectoryError(error_msg)
        return True

    def _files_exist(self) -> bool:
        for file, path in self.files.items():
            error_msg = f"File: {path}"
            if not os.path.exists(path):
                raise FileNotFoundError(error_msg)
            if not os.path.isfile(path):
                raise IsADirectoryError(error_msg)
        return True

    @staticmethod
    def discount(line: dict) -> str:

        def is_number(number: str) -> bool:
            return number.replace('.', '', 1).isdigit()

        price = str(line['price'])
        old_price = str(line['old_price'])
        if not is_number(price) or not is_number(old_price):
            return ''

        price = float(price)
        old_price = float(old_price)
        if price >= old_price:
            return ''

        return f"{100 - price * 100 / old_price:.0f}%"

    @staticmethod
    def shrink_line(line: dict, length: int = 64) -> dict:
        return {key: value[:length]+"..." if len(value) > length else value
                for key, value in line.items()}


class Reader(ReaderBase):

    def read_eans(self) -> list:
        with open(self.files['eans'], 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            return [line['ean'] for line in reader if line['active'] == '1']

    def read_data(self,
                  eans: list = None,
                  shrink: bool = os.environ.get("DEBUG", False)) -> list:
        with gzip.open(self.files['data'], 'rt') as csvfile:
            reader = csv.DictReader(csvfile)
            if eans:
                return [self.shrink_line(line)
                        if shrink else line | {'discount': self.discount(line)}
                        for line in reader if line['ean'] in eans]
            else:
                return [self.shrink_line(line)
                        if shrink else line | {'discount': self.discount(line)}
                        for line in reader]

    @staticmethod
    def print(data: list[dict[str: str]]) -> None:
        print(tabulate(data,
                       headers="keys",
                       showindex=True,
                       tablefmt='psql',
                       numalign='left',
                       stralign='left'))


def main() -> None:
    reader = Reader()
    eans = reader.read_eans()
    products = reader.read_data(eans, shrink=False)
    reader.print(products)


if __name__ == '__main__':
    main()
