import os
import asyncio, asyncssh
from airbyte_cdk.models.airbyte_protocol import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type


class ClientAsync:
    # CONNECTOR_LOCAL_DIR = "/home/husseljo/damn/orange-files/"
    CONNECTOR_LOCAL_DIR = "/local"
    HOST_LOCAL_DIR = "/tmp/airbyte_local/"

    def __init__(self, config):
        self.hdfs_path = str(config.get("hdfs_path"))
        self.consumers_number = str(config.get("consumers_number"))
        self.airbyte_host_username = str(config.get("airbyte_host_username"))
        self.airbyte_host_password = str(config.get("airbyte_host_password"))
        self.airbyte_host_port = str(config.get("airbyte_host_port"))
        self.localmachine_con = None, None
        self.sftp_clients = {}

    async def get_sftp_client(self, con_data):
        host, port, username, password = (
            con_data["host"],
            con_data["port"],
            con_data["username"],
            con_data["password"],
        )
        self.sftp_clients[host] = "in_progress"
        con = await asyncssh.connect(host, port, username=username, password=password, known_hosts=None)
        sftp_client = await con.start_sftp_client()
        self.sftp_clients[host] = sftp_client
        return sftp_client

    async def establish_localmachine_connection(self):
        localmachine_con = await asyncssh.connect(
            "172.17.0.1", 22, username=self.airbyte_host_username, password=self.airbyte_host_password, known_hosts=None
        )
        return localmachine_con

    async def consumer_write_hdfs(self, queue, id):
        print(f"CONSUMER {id} STARTED")
        while True:
            file_path = await queue.get()
            print(f"item in queue: {file_path}")
            # Terminate the consumer when "STOP" is encountered
            if file_path == "STOP":
                break
            file_name = os.path.basename(file_path)
            host_file_path = os.path.join(self.HOST_LOCAL_DIR, file_path)
            connector_file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, file_path)
            # command = f"docker cp {host_file_path} namenode:/ && docker exec namenode hadoop dfs -copyFromLocal -f {file_name} {self.hdfs_path} && docker exec namenode sh -c 'rm {file_name}'"
            command = f"$HADOOP_HOME/bin/hadoop dfs -copyFromLocal -f {host_file_path} {self.hdfs_path}"
            result = await self.localmachine_con.run(command)
            print(f"Consumer {id}, exit_status for {file_name} output:", result.exit_status)
            if result.exit_status == 0 and os.path.exists(connector_file_path):
                os.remove(connector_file_path)
                print(f"File '{file_path}' cleaned up successfully.")
            else:
                print("standard output: ", result.stdout)
                print("standard error: ", result.stderr)
                raise Exception(f"Error while copying {file_name} to hdfs")
            print(f"Consumer {id} had written {file_name} to HDFS")
            queue.task_done()

    async def producer_copy_locally(self, queue, stream_name, data):
        source_host, source_port, source_username, source_password, source_path, file_name = (
            data["host"],
            data["port"],
            data["username"],
            data["password"],
            data["path"],
            data["file_name"],
        )
        print(f"Starting to produce {file_name}\n")
        directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        if source_host not in self.sftp_clients:
            await self.get_sftp_client(data)
        elif self.sftp_clients[source_host] == "in_progress":
            await self.wait_for_sftp_client_connection(source_host)
        remote_file_path = os.path.join(source_path, file_name)
        local_file_path = os.path.join(directory_path, file_name)
        await self.sftp_clients[source_host].get(
            remotepaths=remote_file_path,
            localpath=local_file_path,
        )

        await queue.put(f"{stream_name}/{file_name}")
        print(f"producer has copied {file_name} locally.")

    async def wait_for_sftp_client_connection(self, host):
        while self.sftp_clients[host] == "in_progress":
            await asyncio.sleep(0.1)  # Adjust sleep duration as needed
        return self.sftp_clients[host]

    async def run(self, input_messages):
        self.localmachine_con = await self.establish_localmachine_connection()

        results = []
        queue = asyncio.Queue()
        consumer_num = 3
        consumer_tasks = [asyncio.create_task(self.consumer_write_hdfs(queue, i)) for i in range(consumer_num)]
        producer_tasks = []
        for message in input_messages:
            if message.type == Type.STATE:
                results.append(message)
                print("STATE MESSAGE: ", message)
                continue
            stream_name = message.record.stream
            data = message.record.data
            producer_task = asyncio.create_task(self.producer_copy_locally(queue, stream_name, data))
            producer_tasks.append(producer_task)
        await asyncio.gather(*producer_tasks)
        for _ in range(consumer_num):
            await queue.put("STOP")
        await asyncio.gather(*consumer_tasks)
        print("ALL FINISHED")
        return results
