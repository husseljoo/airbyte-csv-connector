# client.py
import paramiko
from paramiko.sftp_client import SFTPClient
import stat
import datetime


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

    def list_files(
        self, path: str = "", target_time: int = 0, file_extension: str = ""
    ):
        path_to_use = path if path else self.path
        files = self._listdir_sorted_by_modification_time(path_to_use, target_time)
        return files

    def _listdir_sorted_by_modification_time(
        self, path: str = ".", target_time: int = 0
    ):
        current_time = datetime.datetime.now()
        with self.connect() as sftp:
            attrs_list = sftp.listdir_attr(path=path)
            attrs_list = list(
                filter(
                    lambda x: not stat.S_ISDIR(x.st_mode) and x.st_mtime > target_time,
                    attrs_list,
                )
            )
            if self.file_extension:
                file_extension = (
                    self.file_extension[1:]
                    if self.file_extension.startswith(".")
                    else self.file_extension
                )
                attrs_list = list(
                    filter(
                        lambda x: x.filename.endswith(f".{file_extension}"), attrs_list
                    )
                )
            if self.min_file_age:
                min_file_age_cutoff = current_time - self.min_file_age
                attrs_list = [
                    x
                    for x in attrs_list
                    if datetime.datetime.fromtimestamp(x.st_mtime)
                    <= min_file_age_cutoff
                ]
            if self.max_file_age:
                max_file_age_cutoff = current_time - self.max_file_age
                attrs_list = [
                    x
                    for x in attrs_list
                    if datetime.datetime.fromtimestamp(x.st_mtime)
                    >= max_file_age_cutoff
                ]

            attrs_list.sort(key=lambda x: x.st_mtime)
        return attrs_list
