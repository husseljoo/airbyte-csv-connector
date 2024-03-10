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
    Data is written to HDFS in the following format:
        key: stream_name__ab__<record_extraction_timestamp>
        value: a JSON object representing the record's data

    This is because unless a data source explicitly designates a primary key, we don't know what to key the record on.
    Since HDFS allows reading records with certain prefixes, we treat it more like a message queue, expecting the reader to
    read messages with a particular prefix e.g: name__ab__123, where 123 is the timestamp they last read data from.
    """

    CHUNK_SIZE = 5000
    write_buffer = []
    flush_interval = 1000

    def __init__(self, host: str, port: int, destination_path: str):
        self.host = host
        self.port = port
        self.destination_path = destination_path
        self.client = PyWebHdfsClient(host=host, port=str(port))
        self._items_order = []
        self.header_offset = 0
        self.fields = 0

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
        arr = self.client.create_file(
            self.destination_path, f"{header}\n", overwrite=True
        )
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

    def read_catalog(self):
        length = 200
        header = ""

        while "\n" not in header:
            header = self.client.read_file(
                self.destination_path, offset=0, length=length
            ).decode("utf-8")
            length *= 2
        header = header.split("\n")[0]
        self.header_offset = len(bytes(header)) + 1
        self.fields = len(header.split(","))
        return header

    def flush(self):
        data = "\n".join(self.write_buffer)
        # buffer_size = sys.getsizeof(data) + 400
        # print(f"buffer_size: {buffer_size}")
        # arr = self.client.append_file(self.destination_path, data, buffersize=buffer_size)
        arr = self.client.append_file(self.destination_path, data)
        print(f"arr: {arr}")
        self.write_buffer.clear()

    def extract(self):
        return Records(
            self.client,
            destination_path=self.destination_path,
            header_offset=self.header_offset,
            fields=self.fields,
        )


class Records:
    def __init__(self, client, destination_path, header_offset, fields):
        self._client = client
        self.destination_path = destination_path
        self._header_offset = header_offset
        self._fields = fields

    def __iter__(self):
        def _gen2():
            # offset = self._header_offset
            # decoded_line = self.client.read_file(
            #     self.destination_path, offset=offset, length=CHUNK_SIZE
            # ).decode("utf-8")
            # csv_reader = csv.reader(io.StringIO(decoded_line), delimiter=",")

            # I have to parse the line and make it only consist of correct lines (without trailing partial strings )
            # offset += CHUNK_SIZE  # CHUNK_SIZE will differ

            offset = self._header_offset
            decoded_line = self._client.read_file(
                self.destination_path, offset=offset
            ).decode("utf-8")
            csv_reader = csv.reader(io.StringIO(decoded_line), delimiter=",")
            bool = False
            for row in csv_reader:
                print(type(row))
                if not bool:
                    bool = True
                    continue
                yield row

        def _gen():
            records = self._client.fetch_records()
            if records is None or len(records) == 0:
                return

            for k, v in records.items():
                last_key = k
                data = {"key": k, "value": json.dumps(v)}

                yield data

            # fetch data start at last_key inclusive
            while records := self._client.fetch_records(last_key):
                num_records = len(records)

                records_iter = iter(records.items())
                first_key, first_value = next(records_iter)
                if first_key == last_key:
                    if num_records == 1:
                        return
                else:
                    last_key = first_key
                    data = {"key": first_key, "value": json.dumps(first_value)}
                    yield data

                for k, v in records_iter:
                    last_key = k
                    data = {"key": k, "value": json.dumps(v)}

                    yield data

        return _gen2()
