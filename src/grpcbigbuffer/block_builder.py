import json
import os.path
import warnings
from hashlib import sha3_256
from io import BufferedReader
from itertools import zip_longest
from typing import Any, List, Dict, Union, Tuple
from grpcbigbuffer import buffer_pb2
from google.protobuf.message import Message, DecodeError
from google.protobuf.pyext._message import RepeatedCompositeContainer

from grpcbigbuffer.client import generate_random_dir, block_exists, move_to_block_dir
from grpcbigbuffer.utils import Enviroment, CHUNK_SIZE, METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, \
    get_file_hash, create_lengths_tree, encode_bytes


def is_block(bytes_obj: bytes, blocks: List[bytes]):
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


def get_position_length(varint_pos: int, buffer: bytes) -> int:
    """
    Returns the value of the varint at the given position in the Protobuf buffer.
    """
    value = 0
    shift = 0
    index = varint_pos
    while True:
        byte = buffer[index]
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
        index += 1
    return value


def get_hash(block: buffer_pb2.Buffer.Block) -> str:
    for _hash in block.hashes:
        if _hash.type == Enviroment.hash_type:
            return _hash.value.hex()
    raise Exception('gRPCbb: any hash of type ' + Enviroment.hash_type.hex())


def get_block_length(block_id: str) -> int:
    if os.path.isfile(Enviroment.block_dir + block_id):
        return os.path.getsize(Enviroment.block_dir + block_id)
    elif os.path.isdir(Enviroment.block_dir + block_id):
        raise Exception('gRPCbb: error on compute_real_lengths, multiblock blocks dont supported.')
    else:
        raise Exception('gRPCbb: error on compute_real_lengths, block does not in block registry.')

def search_on_message_real(
        message: Message,
        pointers: List[int],
        initial_position: int,
        blocks: List[bytes],
        container: Dict[str, List[int]]
) -> int:
    position: int = initial_position
    for field, value in message.ListFields():
        if isinstance(value, RepeatedCompositeContainer):
            for element in value:
                message_size = search_on_message_real(
                    message=element,
                    pointers=pointers + [position + 1],
                    initial_position=position + 1 + len(encode_bytes(element.ByteSize())),
                    blocks=blocks,
                    container=container
                )
                position += 1 + len(encode_bytes(message_size)) + message_size

        elif isinstance(value, Message):
            message_size = search_on_message_real(
                message=value,
                pointers=pointers + [position + 1],
                initial_position=position + 1 + len(encode_bytes(value.ByteSize())),
                blocks=blocks,
                container=container
            )
            position += 1 + len(encode_bytes(message_size)) + message_size

        elif type(value) == bytes and is_block(value, blocks):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)
            if get_hash(block) in container:
                raise Exception('gRPCbb block builder error, duplicated blocks not supported.')
            container[
                get_hash(block)
            ] = pointers + [position + 1]
            block_length: int = get_block_length(get_hash(block))
            position += 1 + len(encode_bytes(block_length)) + block_length

        elif type(value) == bytes or type(value) == str:
            position += 1 + len(encode_bytes(len(value))) + len(value)

        else:
            try:
                temp_message = type(message)()
                temp_message.CopyFrom(message)
                for field_name, _ in temp_message.ListFields():
                    if field_name.index != field.index:
                        temp_message.ClearField(field_name.name)
                position += temp_message.ByteSize()
            except Exception as e:
                raise Exception('gRPCbb block builder error obtaining the length of a primitive value :' + str(e))
    return position - initial_position


def search_on_message(
        message: Message,
        pointers: List[int],
        initial_position: int,
        blocks: List[bytes],
        container: Dict[str, List[int]]
):
    position: int = initial_position
    for field, value in message.ListFields():
        if isinstance(value, RepeatedCompositeContainer):
            for element in value:
                search_on_message(
                    message=element,
                    pointers=pointers + [position + 1],
                    initial_position=position + 1 + len(encode_bytes(element.ByteSize())),
                    blocks=blocks,
                    container=container
                )
                position += 1 + len(encode_bytes(element.ByteSize())) + element.ByteSize()

        elif isinstance(value, Message):
            search_on_message(
                message=value,
                pointers=pointers + [position + 1],
                initial_position=position + 1 + len(encode_bytes(value.ByteSize())),
                blocks=blocks,
                container=container
            )
            position += 1 + len(encode_bytes(value.ByteSize())) + value.ByteSize()

        elif type(value) == bytes and is_block(value, blocks):
            block = buffer_pb2.Buffer.Block()
            block.ParseFromString(value)
            if get_hash(block) in container:
                raise Exception('gRPCbb block builder error, duplicated blocks not supported.')
            container[
                get_hash(block)
            ] = pointers + [position + 1]
            position += 1 + len(encode_bytes(block.ByteSize())) + block.ByteSize()

        elif type(value) == bytes or type(value) == str:
            position += 1 + len(encode_bytes(len(value))) + len(value)

        else:
            try:
                temp_message = type(message)()
                temp_message.CopyFrom(message)
                for field_name, _ in temp_message.ListFields():
                    if field_name.index != field.index:
                        temp_message.ClearField(field_name.name)
                position += temp_message.ByteSize()
            except Exception as e:
                raise Exception('gRPCbb block builder error obtaining the length of a primitive value :' + str(e))


def compute_real_lengths(tree: Dict[int, Union[Dict, str]], buffer: bytes) -> Dict[int, Tuple[int, int]]:

    def traverse_tree(internal_tree: Dict, internal_buffer: bytes, initial_total_length: int) \
            -> Tuple[int, Dict[int, Tuple[int, int]]]:

        real_lengths: Dict[int, Tuple[int, int]] = {}
        total_tree_length: int = 0
        total_block_length: int = 0
        for key, value in internal_tree.items():
            if isinstance(value, dict):
                initial_length: int = get_position_length(key, internal_buffer)
                real_length, internal_lengths = traverse_tree(
                    value, internal_buffer, initial_length
                )
                real_lengths[key] = (real_length, 1)
                real_lengths.update(internal_lengths)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

                block_length: int = initial_length + len(encode_bytes(initial_length)) + 1
                total_block_length += block_length

            else:
                b = buffer_pb2.Buffer.Block()
                h = buffer_pb2.Buffer.Block.Hash()
                h.type = Enviroment.hash_type
                h.value = bytes.fromhex(value)
                b.hashes.append(h)
                b_length: int = len(b.SerializeToString())

                real_length: int = get_block_length(value)
                real_lengths[key] = (real_length, b_length)
                total_tree_length += real_length + len(encode_bytes(real_length)) + 1

                block_length: int = b_length + len(encode_bytes(b_length)) + 1
                total_block_length += block_length

        if initial_total_length < total_block_length:
            raise Exception('Error on compute real lengths, block length cant be greater than the total length',
                            initial_total_length, total_block_length)

        total_tree_length += initial_total_length - total_block_length

        return total_tree_length, real_lengths

    return traverse_tree(tree, buffer, len(buffer))[1]


def generate_buffer(buffer: bytes, lengths: Dict[int, Tuple[int, int]]) -> List[bytes]:
    list_of_bytes: List[bytes] = []
    new_buff: bytes = b''
    i: int = 0
    for key, value in lengths.items():
        if i == key:
            i -= 1
        new_buff += buffer[i:key] + encode_bytes(value[0])
        i = key + 1 + value[1]
        if value[1] > 1:
            list_of_bytes.append(new_buff)
            new_buff = b''

    return list_of_bytes


def generate_id(buffers: List[bytes], blocks: List[bytes]) -> bytes:
    hash_id = sha3_256()
    for buffer, block in zip_longest(buffers, blocks):
        if buffer:
            hash_id.update(buffer)
        if block:
            with BufferedReader(open(Enviroment.block_dir + block.hex(), 'rb')) as f:
                while True:
                    f.flush()
                    piece: bytes = f.read(CHUNK_SIZE)
                    if len(piece) == 0:
                        break
                    hash_id.update(piece)
    return hash_id.digest()


def build_multiblock(
        pf_object_with_block_pointers: Any,
        blocks: List[bytes]
) -> Tuple[bytes, str]:
    container: Dict[str, List[int]] = {}
    search_on_message(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container
    )

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(
        pointer_container=container
    )

    real_lengths: Dict[int, Tuple[int, int]] = compute_real_lengths(
        tree=tree,
        buffer=pf_object_with_block_pointers.SerializeToString()
    )

    new_buff: List[bytes] = generate_buffer(
        buffer=pf_object_with_block_pointers.SerializeToString(),
        lengths=real_lengths
    )

    object_id: bytes = generate_id(
        buffers=new_buff,
        blocks=blocks
    )
    cache_dir: str = generate_random_dir() + '/'
    _json: List[Union[
        int,
        Tuple[str, List[int]]
    ]] = []

    container_real_lengths = {}
    search_on_message_real(
        message=pf_object_with_block_pointers,
        pointers=[],
        initial_position=0,
        blocks=blocks,
        container=container_real_lengths
    )
    for i, (b1, b2) in enumerate(zip_longest(new_buff, container_real_lengths.keys())):
        _json.append(i + 1)
        with open(cache_dir + str(i + 1), 'wb') as f:
            f.write(b1)

        if b2:
            _json.append((
                b2,
                container_real_lengths[b2]
            ))

    with open(cache_dir + METADATA_FILE_NAME, 'w') as f:
        json.dump(_json, f)

    with open(cache_dir + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        f.write(pf_object_with_block_pointers.SerializeToString())

    return object_id, cache_dir


def create_block(file_path: str) -> Tuple[bytes, buffer_pb2.Buffer.Block]:
    file_hash: str = get_file_hash(file_path=file_path)
    if not block_exists(block_id=file_hash):
        if not move_to_block_dir(file_hash=file_hash, file_path=file_path):
            raise Exception('gRPCbb error creating block, file could not be moved.')

    file_hash: bytes = bytes.fromhex(file_hash)

    block = buffer_pb2.Buffer.Block()
    h = buffer_pb2.Buffer.Block.Hash()
    h.type = Enviroment.hash_type
    h.value = file_hash
    block.hashes.append(h)

    return file_hash, block
