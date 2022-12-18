import warnings
from typing import Any, List, Dict, Union, Tuple
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


def compute_real_lengths(tree: Dict[int, Union[Dict, str]]) -> Dict[int, Tuple[int, int]]:
    def get_block_length(block_id: str) -> int:
        return len(block_id)

    def traverse_tree(tree: Dict) -> Tuple[int, Dict[int, Tuple[int, int]]]:
        real_lengths: Dict[int, Tuple[int, int]] = {}
        total_tree_length: int = 0
        for key, value in tree.items():
            if isinstance(value, dict):
                real_length, internal_lengths = traverse_tree(value)
                real_lengths[key] = (real_length, 0)
                real_lengths.update(internal_lengths)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

            else:
                b = buffer_pb2.Buffer.Block()
                h = buffer_pb2.Buffer.Block.Hash()
                h.type = b''
                h.value = bytes.fromhex(value)
                b.hashes.append(h)

                real_length: int = get_block_length(value)
                real_lengths[key] = (real_length, len(b.SerializeToString()))
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

        return total_tree_length, real_lengths

    return traverse_tree(tree)[1]


def generate_buffer(buffer: bytes, lengths: Dict[int, Tuple[int, int]]) -> List[bytes]:
    list_of_bytes: List[bytes] = []
    new_buff: bytes = b''
    i: int = 0
    for key, value in lengths.items():
        new_buff += buffer[i:key] + encode_bytes(value[0])
        i = key + len(encode_bytes(key)) + value[1]

        if value[1] > 0:
            list_of_bytes.append(new_buff)
            new_buff = b''

    return list_of_bytes


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

    real_lengths: Dict[int, Tuple[int, int]] = compute_real_lengths(
        tree=tree
    )

    print('\nreal lengths -> ', real_lengths)

    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    print('\nnew buffer -> ', new_buff)
