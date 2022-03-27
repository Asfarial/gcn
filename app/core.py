"""
Module
with core features used over many other modules
"""
import os
from typing import Callable
from types import NoneType
from app.config import FOLDER, EANS, DATA, cmd_args


class Verbose:
    """
    Class which prints messages,
    if verbose argument was passed
    """
    _verbose = cmd_args.verbose

    def __init__(self, verbose: bool = _verbose) -> None:
        self._verbose = (
            self._verbose if isinstance(verbose, NoneType) else verbose
        )

    @property
    def verbose(self) -> bool:
        """
        Wrapper
        :return:
        """
        return self._verbose

    @classmethod
    def message(cls, msg: str, end: str = "\n") -> None:
        """
        To print
        without creating a Verbose instance
        :param msg:
        :param end:
        :return:
        """
        if cls._verbose:
            print(msg, end=end)

    @classmethod
    def print(cls, func: Callable, *f_args, **f_kwargs) -> None:
        """
        To call some print function
        without creating a Verbose instance
        :param func:
        :param f_args:
        :param f_kwargs:
        :return:
        """
        if cls._verbose:
            func(*f_args, **f_kwargs)

    def _message(self, msg: str, end: str = "\n") -> None:
        """
        To print
        :param msg:
        :param end:
        :return:
        """
        if self.verbose:
            print(msg, end=end)


class FileSystemBase:
    """
    Base Class to Work with file system
    Contains all methods-checks
    To have a stable execution of pipelines
    :return:
    """
    _folder = FOLDER
    _eans = EANS
    _data = DATA

    def __init__(
        self,
        folder: str = _folder,
        eans: str = _eans,
        data: str = _data,
    ) -> None:
        self._folder = self.normalize(folder)
        self._eans = self.normalize(eans)
        self._data = self.normalize(data)
        self._files = {
            key: self._make_path(path)
            for key, path in {"eans": self.eans, "data": self.data}.items()
        }

    @staticmethod
    def normalize(string: str) -> str:
        """
        String normalizer
        :return:
        """
        return string.strip().lower()

    @property
    def folder(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._folder

    @property
    def eans(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._eans

    @property
    def data(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._data

    @property
    def files(self) -> dict:
        """
        Wrapper
        :return:
        """
        return self._files

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
        for path in self.files.values():
            error_msg = f"File: {path}"
            if not os.path.exists(path):
                raise FileNotFoundError(error_msg)
            if not os.path.isfile(path):
                raise IsADirectoryError(error_msg)
        return True
