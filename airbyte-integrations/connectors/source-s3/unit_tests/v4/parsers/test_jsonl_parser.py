#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import io
import json
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type

import pytest
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import RecordParseError
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from source_s3.v4.config import Config
from source_s3.v4.parsers import S3JsonlParser
from unit_tests.sources.file_based.in_memory_files_source import InMemoryFilesStreamReader
from unit_tests.sources.file_based.scenarios.scenario_builder import TestScenarioBuilder
from source_s3.v4.stream_reader import SourceS3StreamReader

logger = Logger("")
jsonl_config = Config(bucket="test", streams=[FileBasedStreamConfig(name="test", file_type="jsonl", validation_policy="Emit Record")])
jsonl_newlines_config = Config(
    bucket="test", streams=[FileBasedStreamConfig(name="test", file_type="jsonl", validation_policy="Emit Record", format=)])

@pytest.mark.parametrize(
    "data, expected_schema",
    [
        pytest.param(
            {
                'col1': [1, 2],
                "col2": [1.1, 2.2],
                "col3": ["val1", "val2"],
                "col4": [True, False],
                "col5": [[1, 2], [3, 4]],
                "col6": [{"col6a": 1}, {"col6b": 2}],
                "col7": [None, None]
            }, {
                "col1": {"type": "integer"},
                "col2": {"type": "number"},
                "col3": {"type": "string"},
                "col4": {"type": "boolean"},
                "col5": {"type": "array"},
                "col6": {"type": "object"},
                "col7": {"type": "null"},
            }, id="various_column_types"
        ),
        pytest.param(
            {
                'col1': [1, None],
                "col2": [1.1, None],
                "col3": ["val1", None],
                "col4": [True, None],
                "col5": [[1, 2], None],
                "col6": [{"col6a": 1}, None],
            }, {
                "col1": {"type": "number"},
                "col2": {"type": "number"},
                "col3": {"type": "string"},
                "col4": {"type": "boolean"},
                "col5": {"type": "array"},
                "col6": {"type": "object"},
            }, id="null_values_in_columns"
        ),
        pytest.param({}, {}, id="no_records"),
    ]
)
def test_infer_schema(config, data: Dict[str, Any], expected_schema: Mapping[str, str]) -> None:
    assert S3JsonlParser().infer_schema(config, data, STREAM_READER, LOGGER) == expected_schema


@pytest.mark.parametrize(
    "config, data, expected_output, expected_error",
    [
        pytest.param(
            JSONL_CONFIG,
            {"col1": ["val1", False]}, None, RecordParseError, id="mixed_types_raises_exception"
        ),
    ]
)
def test_parse_records(config, data: List[Any], expected_output: Mapping[str, str], expected_error: Optional[Type[Exception]]) -> None:
    remote_file, stream_reader = make_remote_file_and_stream_reader(data)
    if expected_error:
        with pytest.raises(expected_error):
            S3JsonlParser().parse_records(config, remote_file, stream_reader, LOGGER)


def make_remote_file_and_stream_reader(contents) -> Tuple[RemoteFile, InMemoryFilesStreamReader]:
    files = {
        "test.jsonl": {
            "contents": contents,
            "last_modified": "2023-06-05T03:54:07.000Z",
        }
    }
    reader = InMemoryFilesStreamReader(files=files, file_type="jsonl")
    return RemoteFile(uri="test.jsonl", last_modified=datetime.now()), reader
