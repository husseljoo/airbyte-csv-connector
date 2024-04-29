import os
import time
import shutil
import asyncio, asyncssh
from airbyte_cdk.models.airbyte_protocol import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from datetime import datetime, timedelta
import pydoop.hdfs as hdfs
import concurrent.futures
from threading import Lock
import gzip


class ClientAsync:
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
        self.compress = config.get("compress", False)
        self.compress_workers_number = config.get("compress_workers_number", 5)
        self.localmachine_con = None
        self.sftp_clients = {}
        self.variables = self._get_variable_dict(config.get("variables", []))
        os.environ["HADOOP_USER_NAME"] = "airbyte"
        self.setup_hadoop_config()

    def setup_hadoop_config(self):
        HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop-3.3.4/")
        core_site = os.path.join(self.CONNECTOR_LOCAL_DIR, "core-site.xml")
        core_site_dest = os.path.join(HADOOP_HOME, "etc", "hadoop", "core-site.xml")
        hdfs_site = os.path.join(self.CONNECTOR_LOCAL_DIR, "hdfs-site.xml")
        hdfs_site_dest = os.path.join(HADOOP_HOME, "etc", "hadoop", "hdfs-site.xml")
        shutil.copyfile(core_site, core_site_dest)
        shutil.copyfile(hdfs_site, hdfs_site_dest)

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

    def _evaluate_hdfs_dest(self, path, filename=None, modification_time=None, is_file=False):
        try:
            extension = ""
            if self.compress and is_file:
                filename, extension = os.path.splitext(filename)
            vars = self.variables.copy()
            if filename:
                vars["filename"] = filename
            if modification_time:
                vars["modification_time"] = datetime.fromtimestamp(modification_time) + timedelta(hours=2)

            def evaluate_string(x):
                return eval(f'f"{x}"', {}, vars)

            while True:
                evaluated_string = evaluate_string(path)
                if evaluated_string == path:
                    break
                path = evaluated_string

            if self.compress and is_file and extension:
                path = f"{path}{extension}"

            return path

        except Exception as e:
            raise ValueError(f"Error evaluating string: {e}")

    def compress_to_gzip(self, data):
        file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, data["file_path"])
        file_name = os.path.basename(file_path)
        zipped_path = file_path + ".gz"
        print("compressing file: ", file_name)

        with open(file_path, "rb") as f_in:
            with gzip.open(zipped_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        if os.path.exists(file_path):
            os.remove(file_path)

        return zipped_path

    async def compress_files_locally(self, id, executor):
        print(f"Compress worker {id} STARTED")
        while True:
            data = await self.compress_queue.get()
            # Terminate the consumer when "STOP" is encountered
            if data == "STOP":
                self.compress_queue.task_done()
                break

            loop = asyncio.get_running_loop()
            zipped_path = await loop.run_in_executor(executor, self.compress_to_gzip, data)
            await self.consumer_queue.put({"file_path": zipped_path, "modification_time": data["modification_time"]})

    # async def consumer_write_hdfs(self, id):
    #     print(f"CONSUMER {id} STARTED")
    #     while True:
    #         data = await self.consumer_queue.get()
    #         # Terminate the consumer when "STOP" is encountered
    #         if data == "STOP":
    #             self.consumer_queue.task_done()
    #             break
    #         file_path, modification_time = data["file_path"], data["modification_time"]
    #         file_name = os.path.basename(file_path)
    #         host_file_path = os.path.join(self.HOST_LOCAL_DIR, file_path)
    #         connector_file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, file_path)
    #         # command = f"docker cp {host_file_path} namenode:/ && docker exec namenode hadoop dfs -copyFromLocal -f {file_name} {self.hdfs_path} && docker exec namenode sh -c 'rm {file_name}'"
    #         dynamic_hdfs_path = self._evaluate_hdfs_dest(self.hdfs_path, filename=file_name, modification_time=modification_time)
    #         command1 = f"$HADOOP_HOME/bin/hadoop dfs -mkdir -p {dynamic_hdfs_path}"
    #         if self.hdfs_file:
    #             dynamic_hdfs_file = self._evaluate_hdfs_dest(self.hdfs_file, filename=file_name, modification_time=modification_time, is_file=True)
    #             dynamic_hdfs_path = os.path.join(dynamic_hdfs_path, dynamic_hdfs_file)
    #         command2 = f"$HADOOP_HOME/bin/hadoop dfs -copyFromLocal -f {host_file_path} {dynamic_hdfs_path}"
    #         commands = f"{command1} && {command2}"
    #         start_time = time.monotonic()
    #         result = await self.localmachine_con.run(commands)
    #         end_time = time.monotonic()
    #         io_blocking_time = end_time - start_time
    #         print(f"IO blocking time for '{file_name}' write to HDFS is: {io_blocking_time} seconds")

    #         print(f"Consumer {id}, exit_status for {file_name} output:", result.exit_status)
    #         if result.exit_status == 0 and os.path.exists(connector_file_path):
    #             os.remove(connector_file_path)
    #             print(f"'{file_name}' cleaned up successfully.")
    #         else:
    #             print("standard output: ", result.stdout)
    #             print("standard error: ", result.stderr)
    #             raise Exception(f"Error while copying {file_name} to hdfs")
    #         print(f"Consumer {id} had written {file_name} to HDFS")
    #         self.consumer_queue.task_done()

    async def producer_copy_file_task(self, id, compress=False):
        print(f"PRODUCER {id} STARTED")
        while True:
            item = await self.producer_queue.get()
            if not item:
                self.producer_queue.task_done()
                break  # equivalent to "STOP"
            stream_name, data = item
            source_host, base_path, file_path, file_name, modification_time = (
                data["host"],
                data["base_path"],
                data["file_path"],
                data["file_name"],
                int(data["modification_time"]),
            )
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
            if compress:
                await self.compress_queue.put(data)
            else:
                await self.consumer_queue.put(data)
            print(f"Producer {id} has copied {file_name} locally.")
            self.producer_queue.task_done()

    async def wait_for_sftp_client_connection(self, host):
        while self.sftp_clients[host] == "in_progress":
            await asyncio.sleep(0.1)  # Adjust sleep duration as needed
        return self.sftp_clients[host]

    def close_connections(self):
        # if self.localmachine_con:
        #     self.localmachine_con.close()
        #     print("closed the localmachine_con")
        if self.sftp_clients:
            for host, sftp_client in self.sftp_clients.items():
                sftp_client.exit()
                print(f"exited sftp client session for host {host}, sftp_client: {sftp_client})")

    def write_file_to_hdfs(self, data):
        modification_time = data["modification_time"]
        file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, data["file_path"])
        file_name = os.path.basename(file_path)

        dynamic_hdfs_path = self._evaluate_hdfs_dest(self.hdfs_path, filename=file_name, modification_time=modification_time)

        handle = hdfs.hdfs(host="default", port=0, user=os.environ.get("HADOOP_USER_NAME", "airbyte"))

        dynamic_hdfs_file = ""
        if self.hdfs_file:
            dynamic_hdfs_file = self._evaluate_hdfs_dest(self.hdfs_file, filename=file_name, modification_time=modification_time, is_file=True)

        with self.lock:
            if handle.exists(dynamic_hdfs_path) and handle.get_path_info(dynamic_hdfs_path)["kind"] != "directory":
                hdfs.rm(dynamic_hdfs_path)
            hdfs.mkdir(dynamic_hdfs_path)

            if dynamic_hdfs_file:
                dynamic_hdfs_path = os.path.join(dynamic_hdfs_path, dynamic_hdfs_file)
            else:
                dynamic_hdfs_path = os.path.join(dynamic_hdfs_path, os.path.basename(file_path))

            if handle.exists(dynamic_hdfs_path):
                hdfs.rm(dynamic_hdfs_path)
        hdfs.put(file_path, dynamic_hdfs_path, mode="w")
        if os.path.exists(file_path):
            os.remove(file_path)

        print(f"{file_name} written to hdfs")

    async def consumer_write_hdfs_pydoop(self, id, executor):
        print(f"PYDOOP CONSUMER {id} STARTED")
        while True:
            data = await self.consumer_queue.get()
            # Terminate the consumer when "STOP" is encountered
            if data == "STOP":
                self.consumer_queue.task_done()
                break
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, self.write_file_to_hdfs, data)

    async def run(self, input_messages):
        # self.localmachine_con = await self.establish_localmachine_connection()

        stream_names = set()
        results = []
        self.consumer_queue, self.producer_queue = asyncio.Queue(), asyncio.Queue()
        self.lock = Lock()

        producer_tasks = [asyncio.create_task(self.producer_copy_file_task(i, self.compress)) for i in range(self.producers_number)]
        consumer_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.consumers_number)
        consumer_tasks = [asyncio.create_task(self.consumer_write_hdfs_pydoop(i, consumer_executor)) for i in range(self.consumers_number)]

        compress_tasks = []
        if self.compress:
            compress_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.compress_workers_number)
            self.compress_queue = asyncio.Queue()
            compress_tasks = [asyncio.create_task(self.compress_files_locally(i, compress_executor)) for i in range(self.compress_workers_number)]

        for message in input_messages:
            if message.type == Type.STATE:
                results.append(message)
                print("STATE MESSAGE: ", message)
                continue
            stream_name = message.record.stream
            stream_names.add(stream_name)
            data = message.record.data
            await self.producer_queue.put((stream_name, data))
        for _ in range(self.producers_number):
            await self.producer_queue.put(None)  # equivalent to "STOP"

        all_tasks = producer_tasks + consumer_tasks + compress_tasks if self.compress else producer_tasks + consumer_tasks
        await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
        await asyncio.gather(*producer_tasks)
        print("finished downloading all files locally.")
        if self.compress:
            all_tasks = consumer_tasks + compress_tasks
            for _ in range(self.compress_workers_number):
                await self.compress_queue.put("STOP")
            await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
            await asyncio.gather(*compress_tasks)
            print("finished compressing all files locally.")

        # await asyncio.gather(*producer_tasks)
        for _ in range(self.consumers_number):
            await self.consumer_queue.put("STOP")
        await asyncio.gather(*consumer_tasks)

        for stream_name in stream_names:
            directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
        print("ALL FINISHED")
        self.close_connections()
        return results
