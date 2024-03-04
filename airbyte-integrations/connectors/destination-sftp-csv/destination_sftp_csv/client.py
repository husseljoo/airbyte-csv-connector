#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import csv
import contextlib
import errno
import json
from typing import Dict, List, TextIO

import paramiko
import smart_open


@contextlib.contextmanager
def sftp_client(
    host: str,
    port: int,
    username: str,
    password: str,
) -> paramiko.SFTPClient:
    with paramiko.SSHClient() as client:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            host,
            port,
            username=username,
            password=password,
            look_for_keys=False,
        )
        sftp = client.open_sftp()
        yield sftp


class SftpClient:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        destination_path: str,
        file_name: str = "",
        port: int = 22,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.destination_path = destination_path
        self.file_name = file_name
        self._files: Dict[str, TextIO] = {}
        self._items_order: Dict[str, list] = {}

    def __enter__(self):
        self.file_name = self.file_name[:-4] if self.file_name.lower().endswith(".csv") else self.file_name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_path(self, stream: str) -> str:
        if not self.file_name:
            return f"{self.destination_path}/airbyte_csv_{stream}.csv"
        return f"{self.destination_path}/{self.file_name}.csv"

    def _get_uri(self, stream: str) -> str:
        path = self._get_path(stream)
        return f"sftp://{self.username}:{self.password}@{self.host}:{self.port}/{path}"

    def _open(self, stream: str) -> TextIO:
        uri = self._get_uri(stream)
        return smart_open.open(uri, mode="a+")

    def close(self):
        for file in self._files.values():
            file.close()

    def write_csv_header(self, stream, line):
        if stream not in self._files:
            self._files[stream] = self._open(stream)
            self._items_order[stream] = line.split(",")
        self._files[stream].write(f"{line}\n")

    def write(self, stream: str, record: Dict) -> None:
        records = [""] * len(self._items_order[stream])
        for i in range(len(records)):
            elem = record[self._items_order[stream][i]]
            elem = f'"{elem}"' if isinstance(elem, (list, dict)) else str(elem)
            records[i] = elem
        line = ",".join(records)
        self._files[stream].write(f"{line}\n")

    def write_check(self, stream: str, record: Dict) -> None:
        if stream not in self._files:
            self._files[stream] = self._open(stream)
        text = json.dumps(record)
        self._files[stream].write(f"{text}\n")

    def delete(self, stream: str) -> None:
        with sftp_client(self.host, self.port, self.username, self.password) as sftp:
            try:
                path = self._get_path(stream)
                sftp.remove(path)
            except IOError as err:
                # Ignore the case where the file doesn't exist, only raise the
                # exception if it's something else
                if err.errno != errno.ENOENT:
                    raise
