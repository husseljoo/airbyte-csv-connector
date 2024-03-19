#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_orange_hdfs import DestinationOrangeHdfs

if __name__ == "__main__":
    DestinationOrangeHdfs().run(sys.argv[1:])
