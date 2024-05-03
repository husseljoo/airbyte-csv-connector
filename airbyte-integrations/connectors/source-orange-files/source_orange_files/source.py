#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import paramiko
import time, os, hashlib
from datetime import datetime, timezone
from typing import Dict, Generator
from zoneinfo import ZoneInfo

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
    AirbyteStateBlob,
    SyncMode,
)
from airbyte_cdk.sources import Source
from source_orange_files.client import SftpClient


class SourceOrangeFiles(Source):

    def parse_config(self, config):
        return (
            str(config.get("host")),
            int(str(config.get("port"))),
            str(config.get("username")),
            str(config.get("password")),
            str(config.get("path")),
        )

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            host, port, username, password, _ = self.parse_config(config)

            with paramiko.Transport((host, port)) as source_transport:
                source_transport.connect(username=username, password=password)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        def generate_unique_hash():
            unique_str = f"{time.time()}{os.urandom(32)}"
            hash_object = hashlib.sha256(unique_str.encode())
            hex_dig = hash_object.hexdigest()
            return hex_dig

        path, host = config.get("path"), config.get("host")
        path, host = path.replace("/", "-"), host.replace(".", "-")
        path = path[1:] if path[0] == "-" else path
        hash_str = "_" + generate_unique_hash()
        stream_name = path + host + hash_str

        # stream_name = "StreamName"
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "modification_time": {"type": "integer"},
                "file_name": {"type": "string"},
                "host": {"type": "string"},
                "port": {"type": "string"},
                "base_path": {"type": "string"},
                "file_path": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"},
            },
        }
        default_cursor_field = ["modification_time"]
        source_defined_primary_key = [["file_path"]]
        supported_sync_modes = ["full_refresh", "incremental"]

        streams.append(
            AirbyteStream(
                name=stream_name,
                json_schema=json_schema,
                source_defined_cursor=True,
                source_defined_primary_key=source_defined_primary_key,
                default_cursor_field=default_cursor_field,
                supported_sync_modes=supported_sync_modes,
            )
        )
        return AirbyteCatalog(streams=streams)

    def read(
        self,
        logger: AirbyteLogger,
        config: json,
        catalog: ConfiguredAirbyteCatalog,
        state: Dict[str, any],
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """
        print("CONFIG:")
        print(config)
        print("\n\n\n\n\n\n\n")
        stream_name, sync_mode = "StreamName", "incremental"

        for configured_stream in catalog.streams:
            stream_name = configured_stream.stream.name
            sync_mode = configured_stream.sync_mode

        prev_latest_mod_time = 0
        start_date = config.get("start_date", None)
        if not state and start_date:
            local_datetime = datetime.strptime(start_date, "%Y-%m-%d") if ":" not in start_date else datetime.strptime(start_date, "%Y-%m-%d %H:%M")
            local_datetime = local_datetime.replace(tzinfo=ZoneInfo("Africa/Cairo"))
            prev_latest_mod_time = local_datetime.timestamp()
        elif state and sync_mode == SyncMode.incremental:
            state_data = dict(state[0].stream.stream_state)
            prev_latest_mod_time = state_data.get(stream_name).get("modification_time")

        # I think it can be better to decouple source and destination (the source can easily change servers,direcories etc. without having to alter the destinaton settings, server hosts etc.)
        host, port, username, password, base_path = self.parse_config(config)
        data = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "base_path": base_path,
        }
        latest_mod_time = prev_latest_mod_time

        sftp_client = SftpClient(config)
        for record in sftp_client.list_files(target_time=prev_latest_mod_time):
            data["file_path"] = record.file_path
            data["file_name"] = record.file_name
            data["modification_time"] = record.modification_time
            latest_mod_time = max(latest_mod_time, record.modification_time)
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=stream_name,
                    data=data,
                    emitted_at=int(datetime.now().timestamp()) * 1000,
                ),
            )

        next_state = {stream_name: {"modification_time": latest_mod_time}}
        yield AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(name=stream_name, namespace=None),
                    stream_state=AirbyteStateBlob.parse_obj(next_state),
                ),
            ),
        )
