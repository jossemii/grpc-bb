import json
import os.path
from typing import Union, List, Tuple, Dict

from grpcbigbuffer.utils import BLOCK_LENGTH, METADATA_FILE_NAME, WITHOUT_BLOCK_POINTERS_FILE_NAME, Enviroment, \
    create_lengths_tree, encode_bytes


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


def compute_wbp_lengths(tree: Dict[int, Union[Dict, str]], file_list: List[str]) -> Dict[int, int]:
    def __rec_compute_wbp_lengths(_tree: Dict[int, Union[Dict, str]], _file_list: List[str]) \
            -> Dict[int, Tuple[int, int]]:
        lengths: Dict[int, Tuple[int, int]] = {}
        for key, value in _tree.items():
            position_length: int = get_varint_at_position(key, _file_list)
            if isinstance(value, Dict):
                pruned_length: int = 0
                for k, v in __rec_compute_wbp_lengths(
                    _tree=value,
                    _file_list=_file_list
                ).items():
                    pruned_length += v[1]
                    lengths[k] = (v[0], 0)

            else:
                pruned_length: int = get_pruned_block_length(value)
            lengths[key] = (
                    position_length - pruned_length,
                    position_length + len(encode_bytes(position_length))
                    - pruned_length - len(encode_bytes(pruned_length))
                )
        return lengths

    return {k: v[0] for k, v in __rec_compute_wbp_lengths(_tree=tree, _file_list=file_list).items()}


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
    print('GENERATE ' + dirname)
    with open(dirname + '/' + METADATA_FILE_NAME, 'r') as f:
        _json: List[Union[
            int,
            Tuple[str, List[int]]
        ]] = json.load(f)

    buffer = b''
    file_list: List[str] = []
    for e in _json:
        if type(e) == int:
            file_list.append(dirname + '/' + str(e))
            with open(dirname + '/' + str(e), 'rb') as file:
                buffer += file.read()
        else:
            if type(e) != list or type(e[0]) != str:
                raise Exception('gRPCbb: Invalid block on _.json file.')
            file_list.append(Enviroment.block_dir + e[0])

    blocks: Dict[str, List[int]] = {t[0]: t[1] for t in _json if type(t) == list}

    tree: Dict[int, Union[Dict, str]] = create_lengths_tree(blocks)

    recalculated_lengths: Dict[int, int] = compute_wbp_lengths(tree=tree, file_list=file_list)

    print('\n_json -> ', _json)
    print('\nbuffer without blocks -< ', buffer)
    print('\nblocks -< ', blocks)
    print('\ntree -> ', tree)
    print('\nrecalculated lengths -> ', recalculated_lengths)

    with open(dirname + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'wb') as f:
        f.write(
            generate_new_buffer(recalculated_lengths, buffer)
        )
