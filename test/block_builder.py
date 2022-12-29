import json
import os
import sys, unittest


sys.path.append('../src/')

from grpcbigbuffer.client import Enviroment
from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock, create_lengths_tree, search_on_message

from grpcbigbuffer.block_driver import get_position_length
from grpcbigbuffer.disk_stream import encode_bytes, decode_bytes, get_tag, get_field


class TestSearchOnMessage(unittest.TestCase):
    def test_search_on_message(self):
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

        self.assertEqual(
            search_on_message(
                message=c,
                blocks=[b'sha256', b'sha512', b'sha3256'],
                pointers=[1],
                initial_position=2,
            ),
            {'b3b6ff268470d79820b94534fbdfe98bb8228139672a3c8a2d1459f6e3ae1b3f': [1, 3],
             '2116617adb19b25299b41da4808cb7e04b6b67586df81c4ca2df4f5181cd75b2': [1, 31, 33],
             '0558b8fbf5d11d1d1a06ec1285f3d19ee9389d3627d36abb45ee71118d83f55a': [1, 31, 53]}
        )


class TestCreateLengthsTree(unittest.TestCase):
    def test_create_lengths_tree(self):
        # Test with a single element
        pointer_container = {'abc': [1, 2, 3]}
        expected_output = {1: {2: {3: 'abc'}}}
        self.assertEqual(create_lengths_tree(pointer_container), expected_output)

        # Test with multiple elements
        pointer_container = {'abc': [1, 2, 3, 5], 'fjk': [1, 8]}
        expected_output = {1: {2: {3: {5: 'abc'}}, 8: 'fjk'}}
        self.assertEqual(create_lengths_tree(pointer_container), expected_output)

        # Test with empty input
        pointer_container = {}
        expected_output = {}
        self.assertEqual(create_lengths_tree(pointer_container), expected_output)


if __name__ == '__main__':
    # unittest.main()

    from grpcbigbuffer.test_pb2 import Test

    block = buffer_pb2.Buffer.Block()
    h = buffer_pb2.Buffer.Block.Hash()
    h.type = Enviroment.hash_type
    h.value = b'sha512'
    block.hashes.append(h)

    block2 = buffer_pb2.Buffer.Block()
    h = buffer_pb2.Buffer.Block.Hash()
    h.type = Enviroment.hash_type
    h.value = b'sha256'
    block2.hashes.append(h)

    block3 = buffer_pb2.Buffer.Block()
    h = buffer_pb2.Buffer.Block.Hash()
    h.type = Enviroment.hash_type
    h.value = b'sha3256'
    block3.hashes.append(h)

    b = Test()
    b.t1 = block2.SerializeToString()
    b.t2 = block.SerializeToString()

    c = Test()
    c.t1 = block3.SerializeToString()
    c.t2 = b''.join([b'ja' for i in range(int(sys.argv[1]))])
    c.t3.CopyFrom(b)

    more_complex = Test()
    more_complex.t1 = b''.join([b'ho' for i in range(int(sys.argv[1]))])
    more_complex.t3.CopyFrom(c)

    more_more_complex = Test()
    more_more_complex.t1 = b''.join([b'la' for i in range(int(sys.argv[1]))])
    more_more_complex.t2 = b''.join([b'abc' for i in range(int(sys.argv[1]))])
    more_more_complex.t3.CopyFrom(more_complex)
    more_more_complex.t4.append(b)
    more_more_complex.t4.append(c)

    ultra_complex = Test()
    ultra_complex.t1 = b''.join([b'jo' for i in range(int(sys.argv[1]))])
    ultra_complex.t2 = b''.join([b'hi' for i in range(int(sys.argv[1]))])
    ultra_complex.t3.CopyFrom(more_more_complex)
    ultra_complex.t4.append(b)
    ultra_complex.t4.append(c)
    ultra_complex.t4.append(more_complex)
    ultra_complex.t4.append(more_more_complex)

    object_id, cache_dir = build_multiblock(
        pf_object_with_block_pointers=ultra_complex,
        blocks=[b'sha256', b'sha512', b'sha3256']
    )

    # Read the buffer.
    buffer = b''
    with open(os.path.join(cache_dir, '_.json'), 'r') as f:
        _json = json.load(f)

    print('\n\n\n _json -> ', _json)

    for element in _json:
        if type(element) == int:
            with open(os.path.join(cache_dir, str(element)), 'rb') as f:
                block = f.read()
                print('\ninter block -> ', block)
                buffer += block

        if type(element) == list:
            with open(os.path.join(Enviroment.block_dir, element[0]), 'rb') as f:
                while True:
                    block = f.read(1024)

                    if not block:
                        break
                    print('b-> ', block)
                    buffer += block

    print('\n total buffer -> ', buffer, len(buffer))
    #print('\n total indices buffer -> ', [(i, bytes([b])) for i, b in enumerate(buffer)])


    print('\n\n')
    object = Test()
    object.ParseFromString(buffer)

    print('\n total object -> ', object)

