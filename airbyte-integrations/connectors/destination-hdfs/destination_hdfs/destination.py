#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping
from collections import defaultdict

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type, DestinationSyncMode

from destination_hdfs.client import HdfsClient

# logger = getLogger("airbyte")


class DestinationHdfs(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        # # SOME LOGIC TO DELETE FILE IF OVERWRITE
        # for configured_stream in configured_catalog.streams:
        #     if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
        #         hdfs.delete(......)

        # if file exists in hdfs and overerite:
        #     delete file and create again then append
        # if file exists and incremental:
        #     append from increment
        # if file does not exists and (overwrite or incremental):
        #     create then append

        # hdfs.create_file(destination_path, "")

        streams = {s.stream.name for s in configured_catalog.streams}
        # logger.info(f"Starting write to HDFS with {len(streams)} streams")

        host = str(config.get("host"))
        port = int(str(config.get("port")))  # will remove this shit later
        username = str(config.get("username"))
        destination_path = str(config.get("destination_path"))

        print(host, port, username, destination_path)

        configured_stream = configured_catalog.streams[0]  # can add this in loop if it makes sense to have multiple streams
        properties = configured_stream.stream.json_schema["properties"].keys()
        csv_header = ",".join(properties)
        stream_name = configured_stream.stream.name
        print(stream_name)
        print(properties)
        print(csv_header)

        writer = HdfsClient(host, port, username, destination_path)

        # if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
        #     writer.clear_file()
        #     writer.write_csv_header(stream_name, csv_header)

        # writer.clear_file()
        writer.write_csv_header(stream_name, csv_header)

        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                buffer_size = writer.get_buffer_size()
                print(f"write_buffer_size is : {buffer_size}")
                writer.flush()
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                if record.stream not in streams:  # should not happen anyway
                    continue
                writer.queue_write_operation(record.stream, record.data)
            else:
                # ignore other message types for now
                continue

        print("Flushing remaining buffer data")
        writer.flush()
        print("DONE")

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # TODO

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
