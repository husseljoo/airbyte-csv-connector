#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys
from typing import Dict
from pywebhdfs.webhdfs import PyWebHdfsClient

# from destination_kvdb.client import KvDbClient


class HdfsClient:
    """
    Data is written to HDFS in the following format:
        key: stream_name__ab__<record_extraction_timestamp>
        value: a JSON object representing the record's data

    This is because unless a data source explicitly designates a primary key, we don't know what to key the record on.
    Since HDFS allows reading records with certain prefixes, we treat it more like a message queue, expecting the reader to
    read messages with a particular prefix e.g: name__ab__123, where 123 is the timestamp they last read data from.
    """

    write_buffer = []
    flush_interval = 1000

    def __init__(self, host: str, port: int, username: str, destination_path: str):
        self.host = host
        self.port = port
        self.username = username
        self.destination_path = destination_path
        self.client = PyWebHdfsClient(host=host, port=str(port), user_name=username)
        self._items_order = []

    def get_buffer_size(self):
        return len(self.write_buffer)

    # def clear_file(self):
    #     print("Clearing file...")
    #     arr = self.client.create_file(self.destination_path, "")
    #     print(f"arr: {arr}")

    def write_csv_header(self, stream_name: str, header: str):
        # print("Deleting file ...")
        # arr = self.client.delete_file_dir(self.destination_path)
        # print(f"arr: {arr}")
        print("Writing csv header ...")
        arr = self._items_order = header.split(",")
        print(f"arr: {arr}")
        print("Creating file in csv header ...")
        arr = self.client.create_file(self.destination_path, f"{header}\n", overwrite=True)
        print(f"arr: {arr}")

    def _record_to_csv(self, record: Dict):
        records = [""] * len(self._items_order)
        for i in range(len(records)):
            elem = record[self._items_order[i]]
            elem = f'"{elem}"' if isinstance(elem, (list, dict)) else str(elem)
            records[i] = elem
        line = ",".join(records)
        return line

    def queue_write_operation(self, stream_name: str, record: Dict):
        line = self._record_to_csv(record)  # in csv format
        # print(f"arr: {line}")
        self.write_buffer.append(line)
        # if len(self.write_buffer) == self.flush_interval:
        #     print("Flush remaining data in buffer.")
        #     self.flush()

    # def batch_write(self, stream_name: str, record: Dict):
    #     # pywebhdfs shit
    #     # stream name seems irrelevant as I only see solution for one stream
    #     # kv_pair = (f"{stream_name}__ab__{written_at}", record)
    #     self.write_buffer.append(record)
    #     if len(self.write_buffer) == self.flush_interval:
    #         self.flush()

    def flush(self):
        data = "\n".join(self.write_buffer)
        # buffer_size = sys.getsizeof(data) + 400
        # print(f"buffer_size: {buffer_size}")
        # arr = self.client.append_file(self.destination_path, data, buffersize=buffer_size)
        arr = self.client.append_file(self.destination_path, data)
        print(f"arr: {arr}")
        self.write_buffer.clear()

    # def delete_stream_entries(self, stream_name: str):
    #     """Deletes all the records belonging to the input stream"""
    #     keys_to_delete = []
    #     for key in self.client.list_keys(prefix=f"{stream_name}__ab__"):
    #         keys_to_delete.append(key)
    #         if len(keys_to_delete) == self.flush_interval:
    #             self.client.delete(keys_to_delete)
    #             keys_to_delete.clear()
    #     if len(keys_to_delete) > 0:
    #         self.client.delete(keys_to_delete)
