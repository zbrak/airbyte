import csv
import fnmatch
import json
import os
import sys
from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
import gzip
from io import IOBase
from typing import Dict, Iterable, List, Set

import boto3
import rclone
import smart_open
from airbyte_cdk.models import AirbyteRecordMessage


@dataclass
class Config:
    bucket: str
    streams: Dict[str, List[str]]  # globs
    aws_s3_access_key_id: str = os.getenv("AWS_S3_ACCESS_KEY_ID")
    aws_s3_secret_access_key: str = os.getenv("AWS_S3_SECRET_ACCESS_KEY")

    def make_boto3_session_config(self) -> Dict:
        return {
            "aws_access_key_id": self.aws_s3_access_key_id,
            "aws_secret_access_key": self.aws_s3_secret_access_key,
        }

@dataclass
class FileType:

    def parse(self, fp) -> Iterable[Dict]:
        pass

    @classmethod
    def from_suffix(cls, suffix: str) -> "FileType":
        if suffix == "jsonl":
            return Jsonl()
        elif suffix == "csv":
            return Csv()
        else:
            raise ValueError(f"Invalid suffix: {suffix}")

class Jsonl(FileType):
    def parse(self, fp) -> Iterable[AirbyteRecordMessage]:
        for line in fp:
            yield json.loads(line)


class Csv(FileType):
    def parse(self, fp) -> Iterable[Dict]:
        yield from csv.DictReader(fp)


class FileCollection(ABC):
    def __init__(self, config: Config):
        """
        config = {
            "streams": [[glob1, glob2], [glob3], ...]
        }
        """
        self.config = config

    def open_file(self, path: str) -> IOBase:
        raise NotImplemented

    def read(self):
        """
        records = [
            {"stream": "stream1", "file": "stream1-file1", "data": {"k1": "v1"}},
            {"stream": "stream1", "file": "stream1-file2", "data": {"k1": "v1"}},
            {"stream": "stream2", "file": "stream2-file1", "data": {"k1": "v1"}},
            ...
        ]
        """
        for stream in self.get_streams():
            for file in stream.get_files():
                for record in file.get_records():
                    sys.stdout.write(AirbyteRecordMessage(stream=stream.name,
                                                          data=record,
                                                          emitted_at=int(datetime.now().timestamp())).json())

    def get_streams(self):
        for name, globs in self.config.streams.items():
            yield Stream(name, globs, self)

    def list_files(self, glob: str) -> Iterable["File"]:
        ls_output = rclone.with_config(make_rclone_config(self.config)).lsjson(f"remote:{self.config.bucket}")
        remote_files = [file["Path"] for file in json.loads(ls_output["out"])]
        for path in fnmatch.filter(remote_files, glob):
            yield File(path, file_collection=self)


def make_rclone_config(config: Config):
    return f"""
    [remote]
    type = s3
    provider = AWS
    env_auth = false
    access_key_id = {config.aws_s3_access_key_id}
    secret_access_key = {config.aws_s3_secret_access_key}
    region = us-west-2
    endpoint =
    location_constraint =
    acl = private
    server_side_encryption =
    storage_class =
    """


@dataclass
class File:
    path: str
    file_collection: FileCollection
    # file_type: FileType = None
    # suffix: str

    def __post_init__(self):
        self.suffix = self.path.rsplit(".")[-1]
        self.file_type = FileType.from_suffix(self.suffix)

    def __hash__(self) -> int:
        return hash(self.path)

    @contextmanager
    def plain_text_opener(self, fp: IOBase) -> IOBase:
        # FIXME: here suffix is "gz",
        #  but above we're assuming the suffix is "json", giving the FileType.
        if self.suffix == "gz":
            with gzip.open(fp) as plain_text_fp:
                yield plain_text_fp
        else:
            yield fp

    def get_records(self) -> Iterable[Dict]:
        with self.file_collection.open_file(self.path) as maybe_compressed_fp:
            with self.plain_text_opener(maybe_compressed_fp) as plain_text_fp:
                yield from self.file_type.parse(plain_text_fp)


@dataclass
class Stream:
    name: str
    globs: List[str]
    file_collection: FileCollection

    def get_files(self) -> Set[File]:
        return reduce(lambda a, b: a | b, (set(self.file_collection.list_files(glob)) for glob in self.globs))


@dataclass
class S3Bucket(FileCollection):
    config: Config

    def open_file(self, path: str) -> IOBase:
        session = boto3.Session(**self.config.make_boto3_session_config())
        url = f"s3://{self.config.bucket}/{path}"
        return smart_open.open(url, transport_params={"client": session.client('s3')})


if __name__ == "__main__":
    _config = Config(bucket="airbyte-test-catherine", streams={"stream-1": ["*.csv"]})
    _bucket = S3Bucket(_config)
    _bucket.read()  # for every stream, read each file & emit its data as AirbyteRecords
