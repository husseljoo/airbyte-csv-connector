# client.py
import paramiko
from paramiko.sftp_client import SFTPClient
import stat
import os
import datetime
from collections import namedtuple


class SftpClient:
    def __init__(self, config):
        self.host = config.get("host")
        self.port = int(str(config.get("port")))
        self.username = config.get("username")
        self.password = config.get("password")
        self.path = config.get("path")
        self.file_extension = config.get("file_extension")
        self.min_file_age = config.get("min_file_age", None)
        self.max_file_age = config.get("max_file_age", None)
        self.recursive_search = config.get("recursive_search", False)
        if self.min_file_age:
            self.min_file_age = self.parse_time_span(self.min_file_age)
        if self.max_file_age:
            self.max_file_age = self.parse_time_span(self.max_file_age)

    def connect(self):
        source_transport = paramiko.Transport((self.host, self.port))
        source_transport.connect(username=self.username, password=self.password)
        return paramiko.SFTPClient.from_transport(source_transport)

    def parse_time_span(self, time_span_string):
        days, hours, minutes, seconds = map(int, time_span_string.split(":"))
        return datetime.timedelta(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )

    def list_files(self, path: str = "", target_time: int = 0):
        path_to_use = path if path else self.path
        files = self._listdir_sorted_by_modification_time(path_to_use, target_time)
        return files

    def _list_files(self, path, recursive_search: bool = True):
        conn = self.connect()
        files = []

        def list_recursively(path, recursive_search):
            res = conn.listdir_attr(path=path)
            for item in res:
                if recursive_search and stat.S_ISDIR(item.st_mode):
                    subdir_path = os.path.join(path, item.filename)
                    list_recursively(subdir_path, recursive_search)
                else:
                    res = (item, os.path.join(path, item.filename))
                    files.append(res)

        list_recursively(path, recursive_search)
        conn.close()
        files.sort(key=lambda x: x[0].st_mtime)
        return files

    def _listdir_sorted_by_modification_time(
        self, path: str = ".", target_time: int = 0
    ):
        current_time = datetime.datetime.now()
        attributes = self._list_files(path, self.recursive_search)
        attributes = list(
            filter(
                lambda x: not stat.S_ISDIR(x[0].st_mode)
                and x[0].st_mtime > target_time,
                attributes,
            )
        )
        # with self.connect() as sftp:
        #     attrs_list = sftp.listdir_attr(path=path)
        #     attrs_list = list(
        #         filter(
        #             lambda x: not stat.S_ISDIR(x.st_mode) and x.st_mtime > target_time,
        #             attrs_list,
        #         )
        #     )
        if self.file_extension:
            file_extension = (
                self.file_extension[1:]
                if self.file_extension.startswith(".")
                else self.file_extension
            )
            attributes = list(
                filter(
                    lambda x: x[0].filename.endswith(f".{file_extension}"), attributes
                )
            )
        if self.min_file_age:
            min_file_age_cutoff = current_time - self.min_file_age
            attributes = [
                x
                for x in attributes
                if datetime.datetime.fromtimestamp(x[0].st_mtime) <= min_file_age_cutoff
            ]
        if self.max_file_age:
            max_file_age_cutoff = current_time - self.max_file_age
            attributes = [
                x
                for x in attributes
                if datetime.datetime.fromtimestamp(x[0].st_mtime) >= max_file_age_cutoff
            ]
        Record = namedtuple(
            "Record", ["base_path", "file_path", "file_name", "modification_time"]
        )
        records = []
        for attr, file_path in attributes:
            record = Record(
                file_name=attr.filename,
                base_path=self.path,
                file_path=file_path,
                modification_time=attr.st_mtime,
            )
            records.append(record)

        return records
