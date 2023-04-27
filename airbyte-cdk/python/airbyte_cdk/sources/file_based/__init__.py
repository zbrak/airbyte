import json
from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
import gzip
import glob as globlib
from io import IOBase
from typing import Dict, Iterable, List, Set

from airbyte_cdk.models import AirbyteRecordMessage




@dataclass
class Config:
    streams: Dict[str, List[str]]  # globs


@dataclass
class FileType:

    def parse(self, fp) -> Iterable[AirbyteRecordMessage]:
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
    ...




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
                    AirbyteRecordMessage(stream=stream.name, data=record, emitted_at=datetime.now()).emit()

    def get_streams(self):
        for name, globs in self.config.streams:
            yield Stream(name, globs, self)

    def list_files(self, glob: str) -> Iterable["File"]:
        for path in globlib.glob(glob):
            yield File(path, file_collection=self)


@dataclass
class File:
    path: str
    file_type: FileType
    file_collection: FileCollection

    def __post_init__(self):
        suffix = self.path.rsplit(".")[-1]
        self.file_type = FileType.from_suffix(suffix)

    @contextmanager
    def plain_text_opener(self, fp: IOBase) -> IOBase:
        # FIXME: here suffix is "gz",
        #  but above we're assuming the suffix is "json", giving the FileType.
        if self.suffix == "gz":
            with gzip.open(fp) as plain_text_fp:
                yield plain_text_fp
        else:
            yield fp

    def get_records(self) -> Iterable[AirbyteRecordMessage]:
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


class S3Bucket(FileCollection):

    def authenticate(self, *args, **kwargs):
        pass


if __name__ == "__main__":
    config = Config(streams={"stream-1": ["*.json"]})
    bucket = S3Bucket(config)
    bucket.read()  # for every stream, read each file & emit its data as AirbyteRecords
