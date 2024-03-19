#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, Mapping

import time
import subprocess
import paramiko
import os
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models.airbyte_protocol import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from destination_orange_hdfs.client import SftpClient


class DestinationOrangeHdfs(Destination):
    def parse_config(self, config):
        return (
            str(config.get("hdfs_path")),
            int(str(config.get("parallelism"))),
        )

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
        hdfs_path = str(config.get("hdfs_path"))
        print(f"hdfs_path: {hdfs_path}")
        sftp_client = SftpClient(hdfs_path)

        # Create SSH client object
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname="localhost", port=22, username="husseljo", password="husseljo")

        # arr = os.listdir("/local/")
        # print(f"arrAAAAAa: {arr}")
        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                print("STATE")
                yield message
            elif message.type == Type.RECORD:
                stream_name = message.record.stream
                print(f"stream_name: {stream_name}")
                data = message.record.data
                print(f"data: {data}")
                source_host, source_port, source_username, source_password, source_path, file_name = (
                    data["host"],
                    data["port"],
                    data["username"],
                    data["password"],
                    data["path"],
                    data["file_name"],
                )
                # local_dir = f"/local/{stream_name}/"
                # # os.makedirs(local_dir, exist_ok=True)
                # local_dir = f"/local/"
                # local_dir = f"//home/husseljo/damn/orange-files/"

                sftp_client.write_file(data, stream_name)
                # source_sftp = sftp_client.get_transport(source_host, source_port, source_username, source_password, stream_name)

                # local_filepath = os.path.join(local_dir, file_name)
                # print(f"local_filepath: {local_filepath}")
                # remote_filepath = os.path.join(source_path, file_name)
                # source_sftp.get(remotepath=remote_filepath, localpath=local_filepath)
                # source_sftp.close()
                # print(f"Finished copying file locally in: {local_filepath}")
                # start_time = time.time()
                # command = f"docker cp /tmp/airbyte_local/{file_name} namenode:/;docker exec namenode hadoop dfs -copyFromLocal {file_name} {hdfs_path}"
                # stdin, stdout, stderr = ssh_client.exec_command(command)
                # exit_status = stdout.channel.recv_exit_status()
                # end_time = time.time()
                # execution_time = end_time - start_time
                # print("Command execution time:", execution_time, "seconds")

                # for line in stdout.readlines():
                #     print(line.strip())
                # for line in stderr.readlines():
                #     print(line.strip())
                # print("Exit Status:", exit_status)

                # try:
                #     # subprocess.run(["hadoop", "fs", "-copyFromLocal", local_filepath, hdfs_path], check=True)
                #     subprocess.run(
                #         ["docker", "exec", "namenode", "hadoop", "fs", "-copyFromLocal", local_filepath, hdfs_path], check=True
                #     )
                #     print(f"Successfully copied {local_filepath} to {hdfs_path}")
                # except subprocess.CalledProcessError as e:
                #     print(f"Failed to copy {local_filepath} to {hdfs_path}. Error: {e}")

                # with paramiko.Transport((source_host, source_port)) as source_transport:
                #     source_transport.connect(username=source_username, password=source_password)
                #     # source_sftp = paramiko.SFTPClient.from_transport(source_transport)

                #     # Construct local file path
                #     local_filepath = os.path.join(local_dir, file_name)
                #     print(f"local_filepath: {local_filepath}")
                #     print(f"source_path: {source_path}")

                #     # Download file
                #     source_sftp.get(source_path, local_filepath)
                #     print(f"Downloaded {source_path} to {local_filepath}")

                #     print(f"Downloading to hdfs now from {local_filepath} to {hdfs_path}")
            else:
                # ignore other message types for now
                continue
        ssh_client.close()

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
