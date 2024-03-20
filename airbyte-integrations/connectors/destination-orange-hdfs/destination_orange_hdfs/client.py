# client.py
import paramiko
import os
from paramiko.sftp_client import SFTPClient


class SftpClient:
    # CONNECTOR_LOCAL_DIR = "/home/husseljo/damn/orange-files/"
    CONNECTOR_LOCAL_DIR = "/local"
    HOST_LOCAL_DIR = "/tmp/airbyte_local/"

    def __init__(self, hdfs_path):
        self.hdfs_path = hdfs_path
        self.transports = {}
        self.local_machine = self._connect_ssh_local_machine()

    def write_file(self, data, stream_name):
        self._copy_file_locally(data, stream_name)
        file_name = data.get("file_name")
        self._copy_to_hdfs(stream_name, file_name)
        print(f"Written file {file_name} to hdfs and cleaned up successfully")

    def _copy_to_hdfs(self, stream_name, file_name):
        file_path = os.path.join(self.HOST_LOCAL_DIR, stream_name, file_name)
        # command = f"hdfs dfs -put {file_path} {self.hdfs_path}"
        command = f"docker cp {file_path} namenode:/;docker exec namenode hadoop dfs -copyFromLocal -f {file_name} {self.hdfs_path}"
        _, stdout, stderr = self.local_machine.exec_command(command)
        for line in stdout.readlines():
            print(line.strip())
        for line in stderr.readlines():
            print(line.strip())
        exit_status = stdout.channel.recv_exit_status()
        print("Exit Status:", exit_status)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status:
            raise Exception(f"Error while copying {file_name} to hdfs")
        file_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File '{file_path}' removed successfully.")
        else:
            print(f"File '{file_path}' does not exist.")
        # _, stdout, _ = self.local_machine.exec_command(f"rm {file_path}")
        # for line in stdout.readlines():
        #     print(line.strip())
        # for line in stderr.readlines():
        #     print(line.strip())
        # exit_status = stdout.channel.recv_exit_status()
        # print("Exit Status:", exit_status)
        # if exit_status:
        #     raise Exception(f"Error removing {file_name} to hdfs")

    def _copy_file_locally(self, data, stream_name):
        source_host, source_port, source_username, source_password, source_path, file_name = self._parse_data(data)
        source_sftp = self._get_transport(source_host, source_port, source_username, source_password, stream_name)

        directory_path = os.path.join(self.CONNECTOR_LOCAL_DIR, stream_name)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        local_filepath = os.path.join(directory_path, file_name)
        remote_filepath = os.path.join(source_path, file_name)
        source_sftp.get(remotepath=remote_filepath, localpath=local_filepath)  # raises exception in failure
        print(f"copied file {file_name} in {local_filepath}!!, used {source_sftp}")

    def _connect_ssh_local_machine(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname="localhost", port=22, username="husseljo", password="husseljo")
        return ssh_client

    def _get_transport(self, host, port, username, password, stream):
        if stream in self.transports:
            return self.transports[stream]
        # open the sockets manually, I will close them at the end
        source_transport = paramiko.Transport((host, port))
        source_transport.connect(username=username, password=password)
        source_sftp = paramiko.SFTPClient.from_transport(source_transport)
        self.transports[stream] = source_sftp
        return source_sftp

    def _parse_data(self, data):
        source_host = data.get("host")
        source_port = data.get("port")
        source_username = data.get("username")
        source_password = data.get("password")
        source_path = data.get("path")
        file_name = data.get("file_name")
        return source_host, source_port, source_username, source_password, source_path, file_name

    def close_all(self):
        for t in self.transports.values():
            t.close()
