import os
import asyncio, asyncssh
from airbyte_cdk.models.airbyte_protocol import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type


class ClientAsync:
    # CONNECTOR_LOCAL_DIR = "/home/husseljo/damn/orange-files/"
    CONNECTOR_LOCAL_DIR = "/local"
    HOST_LOCAL_DIR = "/tmp/airbyte_local/"

    def __init__(self, hdfs_path):
        self.hdfs_path = hdfs_path
        self.localmachine_con, self.sftp_con = None, None

    async def establish_connections(self):
        # host1, port1, username1, password1 = "localhost", 22, "husseljo", "husseljo"
        host1, port1, username1, password1 = "172.17.0.1", 22, "husseljo", "husseljo"
        host2, port2, username2, password2 = "192.168.56.107", 22, "root", "husseljo"
        localmachine_con = await asyncssh.connect(host1, port1, username=username1, password=password1, known_hosts=None)
        server_con = await asyncssh.connect(host2, port2, username=username2, password=password2, known_hosts=None)
        sftp_con = await server_con.start_sftp_client()
        return localmachine_con, sftp_con

    async def consumer_write_hdfs(self, queue, id):
        print(f"CONSUMER {id} STARTED")
        while True:
            file_path = await queue.get()
            print(f"item: {file_path}")
            # Terminate the consumer when "STOP" is encountered
            if file_path == "STOP":
                break
            file_name = os.path.basename(file_path)
            host_file_path = os.path.join(self.HOST_LOCAL_DIR, file_path)
            print(f"host_file_path: {host_file_path}")
            connector_file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, file_path)
            print(f"connector_file_path: {connector_file_path}")
            command = f"docker cp {host_file_path} namenode:/ && docker exec namenode hadoop dfs -copyFromLocal -f {file_name} /python-async-data && docker exec namenode sh -c 'rm {file_name}'"
            result = await self.localmachine_con.run(command)
            print(f"Consumer {id}, exit_status for {file_name} output:", result.exit_status)
            if result.exit_status == 0 and os.path.exists(connector_file_path):
                os.remove(connector_file_path)
                print(f"File '{file_path}' removed successfully.")
            else:
                print("STD_OUTPUTS:")
                print(result.stdout)
                print("STD_ERRORS:")
                print(result.stderr)
                raise Exception(f"Error while copying {file_name} to hdfs")
            print(f"Consumer {id} copied {file_name} HDFS")
            queue.task_done()

    async def producer_copy_locally(self, queue, stream_name, file_name):
        print(f"Starting to produce {file_name}\n\n\n")
        directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        local_file_path = os.path.join(directory_path, file_name)
        await self.sftp_con.get(
            remotepaths=f"/root/sample_data/data1/{file_name}",
            localpath=local_file_path,
        )

        print(f"STARTING to produce {file_name}\n")
        finished_record = f"{file_name}-finished"
        await queue.put(f"{stream_name}/{file_name}")
        print(f"producer_task_copying produced {finished_record}")

    async def run(self, input_messages):
        self.localmachine_con, self.sftp_con = await self.establish_connections()
        results = []

        queue = asyncio.Queue()
        consumer_num = 3
        consumer_tasks = [asyncio.create_task(self.consumer_write_hdfs(queue, i)) for i in range(consumer_num)]
        producer_tasks = []
        for message in input_messages:
            print(f"message: {message}")
            if message.type == Type.STATE:
                results.append(message)
                print("STATE MESSAGE: ", message)
                continue
            stream_name = message.record.stream
            data = message.record.data
            source_host, source_port, source_username, source_password, source_path, file_name = (
                data["host"],
                data["port"],
                data["username"],
                data["password"],
                data["path"],
                data["file_name"],
            )
            producer_task = asyncio.create_task(self.producer_copy_locally(queue, stream_name, file_name))
            producer_tasks.append(producer_task)
        print("\n\n\n\nGATHERING PRODUCERS.....\n\n\n\n")
        await asyncio.gather(*producer_tasks)
        for _ in range(consumer_num):
            await queue.put("STOP")
        print("\n\n\n\nGATHERING CONSUMERS.....\n\n\n\n")
        await asyncio.gather(*consumer_tasks)
        print("ALL FINISHED")
        return results


def main():
    print("Wassup Husseljo!")


if __name__ == "__main__":
    main()
