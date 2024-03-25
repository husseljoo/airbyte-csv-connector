# client.py
import paramiko
from paramiko.sftp_client import SFTPClient
import stat
import re


class SftpClient:
    def __init__(self, config):
        self.host = str(config.get("host"))
        self.port = int(str(config.get("port")))
        self.username = str(config.get("username"))
        self.password = str(config.get("password"))
        self.path = str(config.get("path"))

    def connect(self):
        source_transport = paramiko.Transport((self.host, self.port))
        source_transport.connect(username=self.username, password=self.password)
        return paramiko.SFTPClient.from_transport(source_transport)

    def list_files(
        self, path: str = "", target_time: int = 0, file_extension: str = ""
    ):
        path_to_use = path if path else self.path
        files = self._listdir_sorted_by_modification_time(
            path_to_use, target_time, file_extension=file_extension
        )
        return files

    def _listdir_sorted_by_modification_time(
        self, path: str = ".", target_time: int = 0, file_extension: str = ""
    ):
        with self.connect() as sftp:
            attrs_list = sftp.listdir_attr(path=path)
            attrs_list = list(
                filter(
                    lambda x: not stat.S_ISDIR(x.st_mode) and x.st_mtime > target_time,
                    attrs_list,
                )
            )
            if file_extension:
                if file_extension.startswith("."):
                    file_extension = file_extension[1:]
                attrs_list = list(
                    filter(
                        lambda x: x.filename.endswith(f".{file_extension}"), attrs_list
                    )
                )

            attrs_list.sort(key=lambda x: x.st_mtime)
        return attrs_list
