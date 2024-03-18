# client.py
import paramiko
from paramiko.sftp_client import SFTPClient
import stat


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

    def list_files(self, path: str = "", target_time: int = 0):
        path_to_use = path if path else self.path
        files = self._listdir_sorted_by_modification_time(path_to_use, target_time)
        return files

    def _listdir_sorted_by_modification_time(
        self, path: str = ".", target_time: int = 0
    ):
        with self.connect() as sftp:
            attrs_list = sftp.listdir_attr(path=path)
            attrs_list = list(filter(lambda x: not stat.S_ISDIR(x.st_mode), attrs_list))
            attrs_list.sort(key=lambda x: x.st_mtime)
            index = self._binary_search_attrs(attrs_list, target_time)
            sliced_list = attrs_list[index:]
        return sliced_list

    def _binary_search_attrs(self, sorted_attrs, target_time):
        l, r = 0, len(sorted_attrs) - 1
        while l <= r:
            mid = (l + r) // 2
            if sorted_attrs[mid].st_mtime <= target_time:
                l = mid + 1
            else:
                r = mid - 1
        return l
