import sys;

sys.path.append('../')

import warnings
from typing import Any, List, Dict
from grpcbigbuffer import buffer_pb2
from google.protobuf.message import Message, DecodeError

from grpcbigbuffer.disk_stream import encode_bytes


def is_block(bytes_obj, blocks: List[bytes]):
    try:
        block = buffer_pb2.Buffer.Block()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            block.ParseFromString(bytes_obj)
        if len(block.hashes) != 0 and block.hashes[0].value in blocks:
            return True
    except DecodeError:
        pass
    return False

def get_hash(block: buffer_pb2.Buffer.Block) -> str:
    from hashlib import sha3_256
    return sha3_256(block.SerializeToString()).hexdigest()


def search_on_message(
        message: Message,
        pointers: List[int],
        initial_position: int,
        blocks: List[bytes]
) -> Dict[str, List[int]]:
    container: Dict[str, List[int]] = {}
    position: int = initial_position
    for field, value in message.ListFields():

        if isinstance(value, Message):
            container.update(
                search_on_message(
                    message=value,
                    pointers=pointers + [position + 1],
                    initial_position=position + 1 + len(encode_bytes(value.ByteSize())),
                    blocks=blocks
                )
            )
            position += 1 + len(encode_bytes(value.ByteSize())) + value.ByteSize()

        elif is_block(value, blocks):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)
            container[
                get_hash(block)
            ] = pointers + [position + 1]
            position += 1 + len(encode_bytes(block.ByteSize())) + block.ByteSize()

        else:
            position += 1 + len(encode_bytes(len(value))) + len(value)

    return container


def build_multiblock(
        pf_object_with_block_pointers: Any,
        blocks: List[bytes]
):
    container: Dict[str, List[int]] = search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[1],
        initial_position=2,
        blocks=blocks
    )
    return container


from grpcbigbuffer.test_pb2 import Test

block = buffer_pb2.Buffer.Block()
h = buffer_pb2.Buffer.Block.Hash()
h.type = b'sha256'
h.value = b'sha512'
block.hashes.append(h)

block2 = buffer_pb2.Buffer.Block()
h = buffer_pb2.Buffer.Block.Hash()
h.type = b'sha256'
h.value = b'sha256'
block2.hashes.append(h)

block3 = buffer_pb2.Buffer.Block()
h = buffer_pb2.Buffer.Block.Hash()
h.type = b'sha256'
h.value = b'sha3256'
block3.hashes.append(h)

b = Test()
b.t1 = block2.SerializeToString()
b.t2 = block.SerializeToString()

c = Test()
c.t1 = block3.SerializeToString()
c.t2 = b'adios'
c.t3.CopyFrom(b)

print(
    build_multiblock(
        pf_object_with_block_pointers=c,
        blocks=[b'sha256', b'sha512', b'sha3256']
    )
)
print(c.SerializeToString())

