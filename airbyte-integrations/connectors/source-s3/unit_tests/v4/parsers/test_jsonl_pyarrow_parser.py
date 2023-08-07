#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import io
import json
from typing import Any, Dict, Mapping

import pandas as pd
import pyarrow as pa
import pytest
from airbyte_cdk.sources.file_based.exceptions import RecordParseError
from source_s3.v4.parsers.jsonl_pyarrow_parser import JsonlPyarrowParser


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
def test_infer_schema(data: Dict[str, Any], expected_schema: Mapping[str, str]) -> None:
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    assert JsonlPyarrowParser._get_schema_for_table(table) == expected_schema


@pytest.mark.parametrize(
    "data, expected_schema",
    [
        pytest.param(
            {"col1": ["val1", False]}, None, id="mixed_types_raises_exception"
        ),
    ]
)
def test_read_table_errors_on_mixed_data_types(data: Dict[str, Any], expected_schema: Mapping[str, str]) -> None:
    # This test demonstrates the current behavior when pyarrow reads a jsonl file whose
    # contents contain a mix of types for a given column.
    fh = io.BytesIO()

    for line in data:
        fh.write((json.dumps(line) + '\n').encode("utf-8"))

    fh.seek(0)
    with pytest.raises(RecordParseError):
        JsonlPyarrowParser()._read_table(fh)
