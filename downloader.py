import os
import requests
from core import FileSystemBase, Verbose


class GoogleDriveDownloader(FileSystemBase, Verbose):
    id_eans = '1UJscscp5gz8WwWq11yYxpPzHFVugGco7'
    id_data = '1j-CSzWiX7YMa0R7LBkbHHYxMDi5YlYjx'
    source = "https://drive.google.com/uc?id={file_id}&export=download&confirm=t"

    chunk_size = 655360

    def __init__(self, *args, **kwargs) -> None:

        Verbose.__init__(self, kwargs.pop('verbose', None))
        FileSystemBase.__init__(self, *args, **kwargs)
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
