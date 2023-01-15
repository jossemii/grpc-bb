import hashlib
import os
from threading import Condition

import typing


# GrpcBigBuffer.
CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_DIR = 999999999
WITHOUT_BLOCK_POINTERS_FILE_NAME = 'wbp.bin'
METADATA_FILE_NAME = '_.json'
BLOCK_LENGTH = 36


class EmptyBufferException(Exception):
    pass


class Dir(object):
    def __init__(self, dir: str):
        self.name = dir


class MemManager(object):
    def __init__(self, len):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        pass


def get_file_hash(file_path: str) -> str:
    # Create a hash object
    hash = hashlib.sha3_256()
    # Open the file in binary mode
    with open(file_path, 'rb') as file:
        # Read the contents of the file in chunks
        chunk = file.read(1024)
        while chunk:
            # Update the hash with the chunk
            hash.update(chunk)
            # Read the next chunk
            chunk = file.read(1024)
        # Calculate the final hash
        file_hash: str = hash.hexdigest()
        # Return the hash
        return file_hash


class Signal():
    # The parser use change() when reads a signal on the buffer.
    # The serializer use wait() for stop to send the buffer if it've to do it.
    # It's thread safe because the open var is only used by one thread (the parser) with the change method.
    def __init__(self, exist: bool = True) -> None:
        self.exist = exist
        if exist: self.open = True
        if exist: self.condition = Condition()

    def change(self):
        if self.exist:
            if self.open:
                self.open = False  # Stop the input buffer.
            else:
                with self.condition:
                    self.condition.notify_all()
                self.open = True  # Continue the input buffer.

    def wait(self):
        if self.exist and not self.open:
            with self.condition:
                self.condition.wait()


## Enviroment ##

class Enviroment(type):
    # Using singleton pattern
    _instances = {}
    cache_dir = os.path.abspath(os.curdir) + '/__cache__/grpcbigbuffer/'
    block_dir = os.path.abspath(os.curdir) + '/__block__/'
    block_depth = 1
    mem_manager = lambda _len: MemManager(len=_len)
    # SHA3_256
    hash_type: bytes = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(Enviroment, cls).__call__()
        return cls._instances[cls]


def modify_env(
        cache_dir: typing.Optional[str] = None,
        mem_manager: typing.Optional[MemManager] = None,
        hash_type: typing.Optional[bytes] = None,
        block_depth: typing.Optional[int] = None,
        block_dir: typing.Optional[str] = None
):
    if cache_dir: Enviroment.cache_dir = cache_dir + 'grpcbigbuffer/'
    if mem_manager: Enviroment.mem_manager = mem_manager
    if hash_type: Enviroment.hash_type = hash_type
    if block_depth: Enviroment.block_depth = block_depth
    if block_dir: Enviroment.block_dir = block_dir
