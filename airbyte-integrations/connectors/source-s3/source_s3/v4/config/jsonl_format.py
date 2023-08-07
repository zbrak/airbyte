#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.file_based.config.jsonl_format import JsonlFormat
from pydantic import Field


class S3JsonlFormat(JsonlFormat):
    newlines_in_values: bool = Field(
        title="Allow newlines in values",
        default=False,
        description="Whether newline characters are allowed in JSON values. Turning this on may affect performance.",
    )
