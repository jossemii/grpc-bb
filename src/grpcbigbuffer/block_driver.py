import json
import os.path
from typing import Union, List, Tuple, Dict

WITHOUT_BLOCK_POINTERS_FILE_NAME = 'wbp.bin'
METADATA_FILE_NAME = '_.json'
BLOCK_LENGTH = 36


def transform_dictionary_format(d: Dict[str, List[int]]) -> Dict[int, List[str]]:
    return {valor: [clave for clave in d if valor in d[clave]] for valor in
            set([valor for clave in d for valor in d[clave]])}


def get_pruned_block_length(block_name: str) -> int:
    return os.path.getsize(block_name) - BLOCK_LENGTH


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


def recalculate_block_length(position: int, blocks_names: List[str], buffer: bytes) -> int:
    return get_position_length(position, buffer) - sum([
        get_pruned_block_length(block_name) for block_name in blocks_names
    ])


def generate_wbp_file(dirname: str) -> bytes:
    with open(dirname + '/' + METADATA_FILE_NAME, 'r') as f:
        _json: List[Union[
            int,
            Tuple[str, List[int]]
        ]] = json.load(f)

    buffer: bytes = b''.join([
        open(dirname + '/' + str(e), 'rb').read() for e in _json if type(e) == int
    ])

    blocks: Dict[str, List[int]] = {t[0]: t[1] for t in _json if type(t) == tuple}

    lengths_with_pointers: Dict[int, List[str]] = transform_dictionary_format(blocks)

    recalculated_lengths: Dict[int, int] = {
        length_position: recalculate_block_length(length_position, blocks_names, buffer) \
        for length_position, blocks_names in lengths_with_pointers.items()
    }
