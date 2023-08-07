#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Dict, Iterable

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.file_types.jsonl_parser import JsonlParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from source_s3.v4.parsers.jsonl_pyarrow_parser import JsonlPyarrowParser


class S3JsonlParser(FileTypeParser):
    jsonl_parser = JsonlParser()
    jsonl_pyarrow_parser = JsonlPyarrowParser()

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> Dict[str, Any]:
        """
        If the legacy 'newlines_in_values' option is set, use the pyarrow parser to infer the schema.

        Otherwise, use the standard JSONL parser.
        """
        if config.format.get("newlines_in_values"):
            yield self.jsonl_pyarrow_parser.infer_schema(config, file, stream_reader, logger)
        else:
            yield self.jsonl_parser.infer_schema(config, file, stream_reader, logger)

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> Iterable[Dict[str, Any]]:
        """
        If the legacy 'newlines_in_values' option is set, use the pyarrow parser to parse records.

        Otherwise, use the standard JSONL parser.
        """
        if config.format.get("newlines_in_values"):
            yield self.jsonl_pyarrow_parser.parse_records(config, file, stream_reader, logger)
        else:
            yield self.jsonl_parser.parse_records(config, file, stream_reader, logger)
