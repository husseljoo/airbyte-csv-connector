# SFTP CSV

## Overview

This destination writes csv data to a directory on an SFTP server.

### Sync Overview

#### Output schema

Each stream will be output into its own file. Each file will contain a collection of CSV rows, which
correspond directly with the data supplied by the source.

#### Features

To be reviewed

| Feature                   | Supported |
| :------------------------ | :-------- |
| Full Refresh Sync         | Yes       |
| Incremental - Append Sync | Yes       |
| Namespaces                | No        |

#### Performance considerations

This integration will be constrained by the connection speed to the SFTP server and speed at which
that server accepts writes.

## Getting Started

The `destination_path` can refer to any path that the associated account has write permissions to.

The `filename` **should not** have an extension in the configuration, as `.csv` will be added on by
the connector.

### Example:

If `destination_path` is set to `/myfolder/files` and `filename` is set to `mydata`, the resulting
file will be `/myfolder/files/mydata.jsonl`.

These files can then be accessed by creating an SFTP connection to the server and navigating to the
`destination_path`.
