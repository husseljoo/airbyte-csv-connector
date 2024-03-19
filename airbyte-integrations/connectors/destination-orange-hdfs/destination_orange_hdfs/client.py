# client.py
import paramiko
from paramiko.sftp_client import SFTPClient


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

    def read_files(self, path: str = None):
        path_to_use = path if path else self.path
        with self.connect() as source_sftp:
            return source_sftp.listdir_attr(path_to_use)
