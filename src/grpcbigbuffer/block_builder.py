from typing import Any, List, Dict
import buffer_pb2
from google.protobuf.message import Message

from grpcbigbuffer.disk_stream import encode_bytes


def get_hash(block: buffer_pb2.Buffer.Block) -> str:
    return ''


def search_on_message(
        message: Message,
        pointers: List[int],
        initial_position: int
) -> Dict[str, List[int]]:

    container: Dict[str, List[int]] = {}
    position: int = initial_position
    for field, value in message.ListFields():
        if field.ByteSizeLong() != 1+len(encode_bytes(field.ByteSize()))+field.ByteSize(): raise Exception('ERROR.')
        if isinstance(value, buffer_pb2.Buffer.Block):
            container[
                get_hash(value)
            ] = pointers + [position+1]

        elif isinstance(value, Message):
            container.update(
                search_on_message(
                    message=value,
                    pointers=pointers + [position+1],
                    initial_position=position + 1 + len(encode_bytes(field.ByteSize()))
                )
            )

        position += field.ByteSizeLong()

    return container


def build_multiblock(
        pf_object_with_block_pointers: Any
):
    container: Dict[str, List[int]] = search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[1],
        initial_position=2
    )
    print(container)
