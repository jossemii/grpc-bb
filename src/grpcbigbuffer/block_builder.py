from typing import Any, List, Dict
import buffer_pb2
from google.protobuf.message import Message


def get_hash(block: buffer_pb2.Buffer.Block) -> str:
    return ''


def search_on_message(message: Message, pointers: List[int]) -> Dict[str, List[int]]:
    container: Dict[str, List[int]] = {}
    position: int = 0
    for field, value in message.ListFields():
        position += field.ByteSizeLong()
        if isinstance(value, buffer_pb2.Buffer.Block):
            container[
                get_hash(value)
            ] = pointers

        elif isinstance(value, Message):
            search_on_message(
                message=value,
                pointers=pointers + [position]
            )

    return container


def build_multiblock(
        pf_object_with_block_pointers: Any,
        list_of_previous_lengths_position: List[int]
):
    container: Dict[str, List[int]] = search_on_message(
        message=pf_object_with_block_pointers
    )
