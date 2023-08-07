#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
from copy import deepcopy

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.sources.file_based.file_types import default_parsers
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from source_s3.v4 import SourceS3StreamReader
from source_s3.v4.config import Config
from source_s3.v4.parsers import S3JsonlParser


parsers = deepcopy(default_parsers)
parsers["jsonl"] = S3JsonlParser()


if __name__ == "__main__":
    args = sys.argv[1:]
    catalog_path = AirbyteEntrypoint.extract_catalog(args)
    source = FileBasedSource(
        SourceS3StreamReader(),
        Config,
        catalog_path,
        parsers=parsers,
    )
    launch(source, args)
