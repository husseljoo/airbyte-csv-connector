#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
import io
from typing import Dict
from pywebhdfs.webhdfs import PyWebHdfsClient

# from destination_kvdb.client import KvDbClient


class HdfsClient:
    """
    Data is read from HDFS
    """

    CHUNK_SIZE = 5000

    def __init__(self, host: str, port: int, source_path: str):
        self.host = host
        self.port = port
        self.source_path = source_path
        self.client = PyWebHdfsClient(host=host, port=str(port))
        self._items_order = []
        self.header_offset = 0
        self.header_fields = ""

    def check_status(self):
        status = self.client.get_file_dir_status(self.source_path)
        if "FileStatus" not in status:
            return False
        return True

    def read_catalog(self):
        length = 200
        header = ""

        while "\n" not in header:
            header = self.client.read_file(
                self.source_path, offset=0, length=length
            ).decode("utf-8")
            length *= 2
        self.header_fields = header
        header = header.split("\n")[0]
        self.header_offset = len(bytes(header, "utf-8")) + 1
        return header

    def extract(self):
        if not self.header_fields:
            self.header_fields = self.read_catalog()

        return Records(
            self.client,
            source_path=self.source_path,
            header_offset=self.header_offset,
            header_fields=self.header_fields,
        )


class Records:
    def __init__(self, client, source_path, header_offset, header_fields):
        self._client = client
        self.source_path = source_path
        self._header_offset = header_offset
        self._header_fields = header_fields

    def generate_data_dict(self, data):
        return dict(zip(self._header_fields.split(","), data))

    def __iter__(self):
        def _gen():
            offset = self._header_offset
            decoded_line = self._client.read_file(
                self.source_path, offset=offset
            ).decode("utf-8")
            csv_reader = csv.reader(io.StringIO(decoded_line), delimiter=",")
            for row in csv_reader:
                yield self.generate_data_dict(row)

        return _gen()
