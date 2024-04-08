import os
import time
import shutil
import asyncio, asyncssh
from airbyte_cdk.models.airbyte_protocol import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from datetime import datetime


class ClientAsync:
    # CONNECTOR_LOCAL_DIR = "/home/husseljo/damn/orange-files/"
    CONNECTOR_LOCAL_DIR = "/local"
    HOST_LOCAL_DIR = "/tmp/airbyte_local/"

    def __init__(self, config):
        self.hdfs_path = config.get("hdfs_path")
        self.hdfs_file = config.get("hdfs_file")
        self.consumers_number = config.get("consumers_number", 5)
        self.producers_number = config.get("producers_number", 5)
        self.max_requests = config.get("sftp_max_requests", 128)
        self.airbyte_host_username = config.get("airbyte_host_username")
        self.airbyte_host_password = config.get("airbyte_host_password")
        self.airbyte_host_port = config.get("airbyte_host_port", 22)
        self.localmachine_con = None
        self.sftp_clients = {}
        self.variables = self._get_variable_dict(config.get("variables", []))

    def _get_variable_dict(self, variables):
        return {var["variable_name"]: var["variable_value"] for var in variables}

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
        HOSTNAME = "172.17.0.1"
        if self.airbyte_host_username and self.airbyte_host_password:
            print("establishing local machine ssh connection using airbyte_host_username:{airbyte_host_username} and password.")
            con = await asyncssh.connect(
                HOSTNAME, 22, username=self.airbyte_host_username, password=self.airbyte_host_password, known_hosts=None
            )
            return con
        private_key = "/local/airbyte-credentials/airbyte_key"
        # USERNAME = "husseljo"
        USERNAME = "root"
        print("establishing local machine ssh connection using private key in:{private_key}.")
        con = await asyncssh.connect(HOSTNAME, 22, username=USERNAME, client_keys=[private_key], known_hosts=None)
        return con

    def _evaluate_hdfs_dest(self, path, filename=None, modification_time=None):
        try:
            vars = self.variables.copy()
            if filename:
                vars["filename"] = filename
            if modification_time:
                vars["modification_time"] = datetime.fromtimestamp(modification_time)

            def evaluate_string(x):
                return eval(f'f"{x}"', {}, vars)

            while True:
                evaluated_string = evaluate_string(path)
                if evaluated_string == path:
                    break
                path = evaluated_string

            return path

        except Exception as e:
            raise ValueError(f"Error evaluating string: {e}")

    async def consumer_write_hdfs(self, queue, id):
        print(f"CONSUMER {id} STARTED")
        while True:
            data = await queue.get()
            # Terminate the consumer when "STOP" is encountered
            if data == "STOP":
                queue.task_done()
                break
            file_path, modification_time = data["file_path"], data["modification_time"]
            file_name = os.path.basename(file_path)
            host_file_path = os.path.join(self.HOST_LOCAL_DIR, file_path)
            connector_file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, file_path)
            # command = f"docker cp {host_file_path} namenode:/ && docker exec namenode hadoop dfs -copyFromLocal -f {file_name} {self.hdfs_path} && docker exec namenode sh -c 'rm {file_name}'"
            dynamic_hdfs_path = self._evaluate_hdfs_dest(self.hdfs_path, filename=file_name, modification_time=modification_time)
            command1 = f"$HADOOP_HOME/bin/hadoop dfs -mkdir -p {dynamic_hdfs_path}"
            if self.hdfs_file:
                dynamic_hdfs_file = self._evaluate_hdfs_dest(self.hdfs_file, filename=file_name, modification_time=modification_time)
                dynamic_hdfs_path = os.path.join(dynamic_hdfs_path, dynamic_hdfs_file)
            command2 = f"$HADOOP_HOME/bin/hadoop dfs -copyFromLocal -f {host_file_path} {dynamic_hdfs_path}"
            commands = f"{command1} && {command2}"
            start_time = time.monotonic()
            result = await self.localmachine_con.run(commands)
            end_time = time.monotonic()
            io_blocking_time = end_time - start_time
            print(f"IO blocking time for '{file_name}' write to HDFS is: {io_blocking_time} seconds")

            print(f"Consumer {id}, exit_status for {file_name} output:", result.exit_status)
            if result.exit_status == 0 and os.path.exists(connector_file_path):
                os.remove(connector_file_path)
                print(f"'{file_name}' cleaned up successfully.")
            else:
                print("standard output: ", result.stdout)
                print("standard error: ", result.stderr)
                raise Exception(f"Error while copying {file_name} to hdfs")
            print(f"Consumer {id} had written {file_name} to HDFS")
            queue.task_done()

    async def producer_copy_file_task(self, producer_queue, consumer_queue, id):
        print(f"PRODUCER {id} STARTED")
        while True:
            item = await producer_queue.get()
            if not item:
                producer_queue.task_done()
                break  # equivalent to "STOP"
            stream_name, data = item
            source_host, base_path, file_path, file_name, modification_time = (
                data["host"],
                data["base_path"],
                data["file_path"],
                data["file_name"],
                int(data["modification_time"]),
            )
            print(f"Starting to produce {file_name}\n")
            directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            if source_host not in self.sftp_clients:
                await self.get_sftp_client(data)
            elif self.sftp_clients[source_host] == "in_progress":
                await self.wait_for_sftp_client_connection(source_host)
            remote_file_path = file_path
            relative_file_path = os.path.relpath(file_path, base_path)
            local_file_path = os.path.join(directory_path, relative_file_path)

            rel_directory = os.path.dirname(local_file_path)
            if not os.path.exists(rel_directory):
                os.makedirs(rel_directory)

            start_time = time.monotonic()
            await self.sftp_clients[source_host].get(
                remotepaths=remote_file_path,
                localpath=local_file_path,
                max_requests=self.max_requests,
            )
            end_time = time.monotonic()
            io_blocking_time = end_time - start_time
            # print(f"IO blocking time for '{file_name}' copy to local : {io_blocking_time} seconds")

            data = {"file_path": os.path.join(stream_name, relative_file_path), "modification_time": modification_time}
            await consumer_queue.put(data)
            print(f"Producer {id} has copied {file_name} locally.")
            producer_queue.task_done()

    async def wait_for_sftp_client_connection(self, host):
        while self.sftp_clients[host] == "in_progress":
            await asyncio.sleep(0.1)  # Adjust sleep duration as needed
        return self.sftp_clients[host]

    def close_connections(self):
        if self.localmachine_con:
            self.localmachine_con.close()
            print("closed the localmachine_con")
        if self.sftp_clients:
            for host, sftp_client in self.sftp_clients.items():
                sftp_client.exit()
                print(f"exited sftp client session for host {host}, sftp_client: {sftp_client})")

    async def run(self, input_messages):
        self.localmachine_con = await self.establish_localmachine_connection()

        stream_names = set()
        results = []
        consumer_queue, producer_queue = asyncio.Queue(), asyncio.Queue()
        consumer_tasks = [asyncio.create_task(self.consumer_write_hdfs(consumer_queue, i)) for i in range(self.consumers_number)]
        producer_tasks = [
            asyncio.create_task(self.producer_copy_file_task(producer_queue, consumer_queue, i)) for i in range(self.producers_number)
        ]
        for message in input_messages:
            if message.type == Type.STATE:
                results.append(message)
                print("STATE MESSAGE: ", message)
                continue
            stream_name = message.record.stream
            stream_names.add(stream_name)
            data = message.record.data
            await producer_queue.put((stream_name, data))
        for _ in range(self.producers_number):
            await producer_queue.put(None)  # equivalent to "STOP"

        # Run producer and consumer tasks concurrently
        all_tasks = producer_tasks + consumer_tasks
        await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
        print("finished downloading all files locally.")

        # await asyncio.gather(*producer_tasks)
        for _ in range(self.consumers_number):
            await consumer_queue.put("STOP")
        await asyncio.gather(*consumer_tasks)

        for stream_name in stream_names:
            directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
        print("ALL FINISHED")
        self.close_connections()
        return results
