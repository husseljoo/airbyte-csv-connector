#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_hdfs import DestinationHdfs

if __name__ == "__main__":
    DestinationHdfs().run(sys.argv[1:])
