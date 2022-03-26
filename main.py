import os
import csv
import gzip
import requests
from tabulate import tabulate

DEBUG = os.environ.get("DEBUG", False)

FOLDER = './records'
EANS = 'eans.csv'
DATA = 'product_data_0.csv.gz'


class FileSystemBase:
    _folder = FOLDER
    _eans = EANS
    _data = DATA

    def __init__(
            self,
            folder: str = _folder,
            eans: str = _eans,
            data: str = _data,
            verbose: bool = False
    ):
        self._verbose = verbose
        self._folder = self.normalize(folder)
        self._eans = self.normalize(eans)
        self._data = self.normalize(data)
        self._files = {key: self._make_path(path) for key, path in {'eans': self.eans, 'data': self.data}.items()}

    @staticmethod
    def normalize(string: str):
        return string.strip().lower()

    @property
    def verbose(self) -> bool:
        return self._verbose

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

    def _message(self, msg: str, end: str = '\n') -> None:
        if self.verbose:
            print(msg, end=end)

    def _make_path(self, file: str) -> str:
        return os.path.join(self.folder, file)

    def _folder_exists(self, raise_for_class: bool = True) -> bool:
        error_msg = f"Folder: {self.folder}"
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
            if raise_for_class:
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


class GoogleDriveDownloader(FileSystemBase):
    id_eans = '1UJscscp5gz8WwWq11yYxpPzHFVugGco7'
    id_data = '1j-CSzWiX7YMa0R7LBkbHHYxMDi5YlYjx'
    source = "https://drive.google.com/uc?id={file_id}&export=download&confirm=t"

    chunk_size = 655360

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._message("Initializing Downloader")
        self._message("Initializing File System checks")
        self._folder_exists(raise_for_class=False)
        self._message("File system checks - OK")

    def make_link(self, file_id: str) -> str:
        return self.source.format(file_id=file_id)

    @staticmethod
    def get_file_web_length(response: requests.Response) -> int:
        return int(response.headers.get("content-length", 0))

    @staticmethod
    def file_is_downloaded(file: str, file_web_length: int) -> bool:
        if os.path.exists(file):
            if os.path.getsize(file) == file_web_length:
                return True
        return False

    def download(self, link: str, output: str, chunk_size: int = chunk_size) -> str:
        self._message(f"Downloading file: {output} ", end="")

        with requests.get(link, stream=True) as response:
            response.raise_for_status()

            file_web_length = self.get_file_web_length(response)
            self._message(
                f"{f'{length * 1024:.2f} KB' if (length := file_web_length / 1024 / 1024) < 1 else f'{length:.2f} MB'}\n{link}")

            if self.file_is_downloaded(output, file_web_length):
                self._message("File is already downloaded. Skipping")
                return ''

            with open(output, 'wb') as file:
                progress = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
                    progress += chunk_size
                    progress = min(file_web_length, progress)
                    self._message(
                        f"\r{progress * 100 / file_web_length:.0f}%",
                        end="",
                    )
                self._message("")
        return output


class ReaderBase(FileSystemBase):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._message("Initializing File System checks")
        self._folder_exists()
        self._files_exist()
        self._message("File system checks - OK")

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
        return {key: value[:length] + "..." if len(value) > length else value
                for key, value in line.items()}

    @staticmethod
    def shrink_list(list: list, length: int = 64) -> list:
        return [Reader.shrink_line(item, length) for item in list]


class Reader(ReaderBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._message("Initializing Reader")

    def read_eans(self) -> list:
        self._message("Reading Eans")
        with open(self.files['eans'], 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            return [line['ean'] for line in reader if line['active'] == '1']

    def read_data(self,
                  eans: list = None) -> list:
        self._message(f"Reading Data. Filter: {'eans - active only' if eans else 'all records'}")
        with gzip.open(self.files['data'], 'rt') as csvfile:
            reader = csv.DictReader(csvfile)
            if eans:
                return [line | {'discount': self.discount(line)}
                        for line in reader if line['ean'] in eans]
            else:
                return [line | {'discount': self.discount(line)}
                        for line in reader]

    @staticmethod
    def print(data: list[dict[str: str]],
              shrink: bool = DEBUG) -> None:
        data = Reader.shrink_list(data) if shrink else data
        print(tabulate(data,
                       headers="keys",
                       showindex=True,
                       tablefmt='psql',
                       numalign='left',
                       stralign='left'))


def downloader_pipeline(verbose: bool = False) -> None:
    if verbose:
        print("Starting Downloader Pipeline")

    downloader = GoogleDriveDownloader(verbose=verbose)
    link_eans = downloader.make_link(downloader.id_eans)
    link_data = downloader.make_link(downloader.id_data)
    downloader.download(link_eans, downloader.files['eans'])
    downloader.download(link_data, downloader.files['data'])


def reader_pipeline(verbose: bool = False) -> None:
    if verbose:
        print("Starting Reader Pipeline")

    reader = Reader(verbose=verbose)
    eans = reader.read_eans()
    products = reader.read_data(eans)

    if verbose:
        reader.print(products)


def main() -> None:

    verbose = True
    downloader_pipeline(verbose=verbose)
    reader_pipeline(verbose=verbose)




if __name__ == '__main__':
    main()
