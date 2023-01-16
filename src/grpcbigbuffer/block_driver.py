import json
import os.path
from typing import Union, List, Tuple, Dict

from grpcbigbuffer.disk_stream import encode_bytes
from grpcbigbuffer.utils import BLOCK_LENGTH, METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, Enviroment


def transform_dictionary_format(d: Dict[str, List[int]]) -> Dict[int, List[str]]:
    return {valor: [clave for clave in d if valor in d[clave]] for valor in
            set([valor for clave in d for valor in d[clave]])}


def get_pruned_block_length(block_name: str) -> int:
    block_size: int = os.path.getsize(Enviroment.block_dir + block_name)
    return block_size + len(encode_bytes(block_size)) - BLOCK_LENGTH - len(encode_bytes(BLOCK_LENGTH))


def get_varint_at_position(position, file_list):
    file_size = sum(os.path.getsize(f) for f in file_list)
    if position > file_size:
        raise ValueError(f"Position {position} is out of buffer range.")
    file_index = 0
    while position > os.path.getsize(file_list[file_index]):
        position -= os.path.getsize(file_list[file_index])
        file_index += 1
    with open(file_list[file_index], "rb") as file:
        file.seek(position)
        result = 0
        shift = 0
        while True:
            byte = file.read(1)
            if not byte:
                break
            byte = ord(byte)
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        return result


def recalculate_block_length(position: int, blocks_names: List[str], file_list: List[str]) -> int:
    print('\nposition -> ', position)
    position_length: int = get_varint_at_position(position, file_list)
    print('position length -> ', position_length)
    pruned_length: int = sum([
        get_pruned_block_length(block_name) for block_name in blocks_names
    ])
    print('pruned length -> ', pruned_length)
    return position_length - pruned_length


def set_varint_value(varint_pos: int, buffer: bytes, new_value: int) -> bytes:
    """
    Sets the value of the varint at the given position in the Protobuf buffer to the given value and returns the modified buffer.
    """
    # Convert the given value to a varint and store it in a bytes object
    varint_bytes = []
    while True:
        byte = new_value & 0x7F
        new_value >>= 7
        varint_bytes.append(byte | 0x80 if new_value > 0 else byte)
        if new_value == 0:
            break
    varint_bytes = bytes(varint_bytes)

    # Calculate the number of bytes to remove from the original varint
    original_varint_bytes = buffer[varint_pos:]
    original_varint_length = 0
    while (original_varint_bytes[original_varint_length] & 0x80) != 0:
        original_varint_length += 1
    original_varint_length += 1

    # Remove the original varint and append the new one
    return buffer[:varint_pos] + varint_bytes + buffer[varint_pos + original_varint_length:]


def generate_new_buffer(lengths: Dict[int, int], buffer: bytes) -> bytes:
    for varint_pos, new_value in lengths.items():
        buffer = set_varint_value(varint_pos, buffer, new_value)

    return buffer


def generate_wbp_file(dirname: str):
    with open(dirname + '/' + METADATA_FILE_NAME, 'r') as f:
        _json: List[Union[
            int,
            Tuple[str, List[int]]
        ]] = json.load(f)

    # buffer = b''.join([i for i in read_multiblock_directory(dirname)]) FOR ALL THE BUFFER
    buffer = b''
    file_list: List[str] = []
    for e in _json:
        if type(e) == int:
            file_list.append(str(e))
            with open(dirname + '/' + str(e), 'rb') as file:
                buffer += file.read()
        else:
            if type(e) != list or type(e[0]) != str:
                raise Exception('gRPCbb: Invalid block on _.json file.')
            file_list.append(e[0])

    blocks: Dict[str, List[int]] = {t[0]: t[1] for t in _json if type(t) == list}

    lengths_with_pointers: Dict[int, List[str]] = transform_dictionary_format(blocks)

    recalculated_lengths: Dict[int, int] = {
        length_position: recalculate_block_length(length_position, blocks_names, file_list)
        for length_position, blocks_names in lengths_with_pointers.items()
    }

    print('\n_json -> ', _json)
    print('\nbuffer without blocks -< ', buffer)
    print('\nblocks -< ', blocks)
    print('\nlengths_wth pointers -> ', lengths_with_pointers)
    print('\nrecalculated lengths -> ', recalculated_lengths)

    with open(dirname + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        f.write(
            generate_new_buffer(recalculated_lengths, buffer)
        )
