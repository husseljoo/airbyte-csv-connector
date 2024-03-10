#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceHdfs

def run():
    source = SourceHdfs()
    launch(source, sys.argv[1:])
