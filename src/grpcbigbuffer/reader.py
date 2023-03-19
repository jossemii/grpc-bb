import gc
import json
import os
import shutil
from io import BufferedReader
from typing import Generator, Union

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.utils import Signal, CHUNK_SIZE, METADATA_FILE_NAME, Enviroment


def block_exists(block_id: str, is_dir: bool = False) -> bool:
    try:
        f: bool = os.path.isfile(Enviroment.block_dir + block_id)
        d: bool = os.path.isdir(Enviroment.block_dir + block_id)
    except Exception as e:
        raise Exception(
            'gRPCbb error checking block: ' + str(e) + " " + str(Enviroment.block_dir) + " " + str(
                block_id) + " " + str(is_dir)
        )
    return f or d if not is_dir else (f or d, d)


def read_file_by_chunks(filename: str, signal: Signal = None) -> Generator[bytes, None, None]:
    if not signal: signal = Signal(exist=False)
    signal.wait()
    try:
        with BufferedReader(open(filename, 'rb')) as f:
            while True:
                f.flush()
                signal.wait()
                piece: bytes = f.read(CHUNK_SIZE)
                if len(piece) == 0: return
                yield piece
    finally:
        gc.collect()


def read_multiblock_directory(directory: str, delete_directory: bool = False, ignore_blocks: bool = True) \
        -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    if directory[-1] != '/':
        directory = directory + '/'
    for e in json.load(open(
            directory + METADATA_FILE_NAME,
    )):
        print('READ --> ', e, type(e))
        if type(e) == int:
            yield from read_file_by_chunks(filename=directory + str(e))
        else:
            block_id: str = e[0]
            print('BLOCK ID -> ', block_id)
            if type(block_id) != str:
                raise Exception('gRPCbb error on block metadata file ( _.json ).')
            if not ignore_blocks:
                block = buffer_pb2.Buffer.Block(
                    hashes=[buffer_pb2.Buffer.Block.Hash(type=Enviroment.hash_type, value=bytes.fromhex(block_id))],
                    previous_lengths_position=e[1]
                )
                yield block
                yield from read_block(block_id=block_id)
                yield block
            else:
                for _i in read_block(block_id=block_id):
                    print('block -> ', block_id, len(_i))
                    yield _i
                #yield from read_block(block_id=block_id)

    if delete_directory:
        shutil.rmtree(directory)


def read_block(block_id: str) -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    b, d = block_exists(block_id=block_id, is_dir=True)
    if b and not d:
        yield from read_file_by_chunks(filename=Enviroment.block_dir + block_id)

    elif d:
        yield from read_multiblock_directory(
            directory=Enviroment.block_dir + block_id,
            ignore_blocks=False
        )

    else:
        raise Exception('gRPCbb: Error reading block.')


def read_from_registry(filename: str, signal: Signal = None) -> Generator[buffer_pb2.Buffer, None, None]:
    for c in read_multiblock_directory(
            directory=filename,
            ignore_blocks=False
    ) if os.path.isdir(filename) else \
            read_file_by_chunks(
                filename=filename,
                signal=signal
            ):
        yield buffer_pb2.Buffer(chunk=c) if type(c) is bytes else buffer_pb2.Buffer(block=c)
