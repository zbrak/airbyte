#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from io import IOBase
from typing import Any, Dict, Iterable

import pyarrow as pa
import pyarrow.json as pa_json

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import type_mapping_to_json_types
from pyarrow import ArrowInvalid


class JsonlPyarrowParser(FileTypeParser):

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
        Infers the schema for the file by inferring the schema for each line, and merging
        it with the previously-inferred schema.
        """
        with stream_reader.open_file(file, self.file_read_mode, logger) as fp:
            return self._get_schema_for_table(self._read_table(fp))

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> Iterable[Dict[str, Any]]:

        with stream_reader.open_file(file) as fp:
            table = self._read_table(fp)
        yield from table.to_pylist()

    @classmethod
    def _get_schema_for_table(cls, table):
        def field_type_to_str(type_: Any) -> str:
            if isinstance(type_, pa.lib.StructType):
                return "struct"
            if isinstance(type_, pa.lib.ListType):
                return "list"
            if isinstance(type_, pa.lib.DataType):
                return str(type_)
            raise Exception(f"Unknown PyArrow Type: {type_}")

        type_mapping = {field.name: field_type_to_str(field.type) for field in table.schema}
        schema = type_mapping_to_json_types(type_mapping)
        return schema

    @classmethod
    def _read_table(cls, fp: IOBase) -> pa.Table:
        try:
            return pa_json.read_json(
                fp,
                # Note:
                # The existing source_files_abstract code passes in a `block_size` ReadOption
                # and an `explicit_schema` ParseOption.
                #
                # `explicit_schema` was a schema mapping the types from the json schema created
                # during schema inference to pyarrow types. This was used to allow pyarrow to
                # validate the record against the schema, where the user could configure certain
                # behavior if the record didn't conform. However, we're now allowing users to
                # configure their policy for whether records conform for all file types instead
                # of just JSONL. Additionally, the source_files_abstract code was doing a schema
                # inference before reading files, and passing that value in as the schema to
                # check records against. However, that schema was simply created on-the-fly and
                # is not necessarily the schema that the destination expects, so does not seem
                # particularly useful for validation.
                #
                # `block_size` would enable us to avoid reading the entire file into memory at
                # once. We can revisit setting this option if needed.
                pa.json.ReadOptions(block_size=None, use_threads=True),
                pa.json.ParseOptions(newlines_in_values=True),
            )
        except ArrowInvalid as exc:
            raise RecordParseError(FileBasedSourceError.ERROR_PARSING_RECORD) from exc
