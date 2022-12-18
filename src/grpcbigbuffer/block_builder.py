import warnings
from typing import Any, List, Dict, Union
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


def create_lengths_tree(
        pointer_container: Dict[str, List[int]]
) -> Dict[int, Union[Dict, str]]:
    tree = {}
    for key, pointers in pointer_container.items():
        current_level = tree
        for pointer in pointers[:-1]:
            if pointer not in current_level:
                current_level[pointer] = {}
            current_level = current_level[pointer]
        current_level[pointers[-1]] = key
    return tree


def generate_buffer(tree: Dict[int, Union[Dict, str]], initial_buffer: bytes) -> bytes:
    


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
    print('\npointer container -> ', container)

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    print('\nlengths tree -> ', tree)

    new_buff: bytes = generate_buffer(
        tree=tree,
        initial_buffer=pf_object_with_block_pointers
    )

    print('\nnew buff -> ', new_buff)
