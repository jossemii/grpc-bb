import json
import os
import sys, unittest, json

sys.path.append('../src/')

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import create_lengths_tree, build_multiblock
from grpcbigbuffer.utils import Enviroment
from grpcbigbuffer.disk_stream import encode_bytes


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


class TestBlockBuilder(unittest.TestCase):
    def test_typical_complex_object(self):

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

        a = Test()
        a.t1 = b''.join([b'bt1' for i in range(100)])
        a.t2 = block.SerializeToString()

        b = Test()
        b.t1 = block2.SerializeToString()
        b.t2 = b''.join([b'bt2' for i in range(100)])
        b.t3.CopyFrom(a)

        c = Test()
        c.t1 = b''.join([b'ct1' for i in range(100)])
        c.t2 = block3.SerializeToString()

        object = Test()
        object.t1 = b''.join([b'mc1' for i in range(100)])
        object.t2 = b''.join([b'mc2' for i in range(100)])
        object.t4.append(b)
        object.t4.append(c)

        object_id, cache_dir = build_multiblock(
            pf_object_with_block_pointers=object,
            blocks=[b'sha256', b'sha512', b'sha3256']
        )

        # Read the buffer.
        buffer = b''
        with open(os.path.join(cache_dir, '_.json'), 'r') as f:
            _json = json.load(f)

        for element in _json:
            if type(element) == int:
                with open(os.path.join(cache_dir, str(element)), 'rb') as f:
                    block = f.read()
                    buffer += block

            if type(element) == list:
                with open(os.path.join(Enviroment.block_dir, element[0]), 'rb') as f:
                    while True:
                        block = f.read(1024)

                        if not block:
                            break
                        buffer += block

        buff_object = Test()
        buff_object.ParseFromString(buffer)
        print(buff_object)

        def extract_last_elements(json_obj):
            result = []
            for _element in json_obj:
                if type(_element) == list and len(_element) == 2 and type(_element[0]) == str and type(
                        _element[1]) == list:
                    result.append(_element[1][-1])
            return result

        print(_json)
        from block_driver import get_position_length
        for _e in extract_last_elements(_json):
            print(
                str(_e) + ' ', get_position_length(_e, buffer),
                encode_bytes(get_position_length(_e, buffer)),
                buffer[_e:_e+get_position_length(_e, buffer)+len(encode_bytes(get_position_length(_e, buffer)))]
            )


if __name__ == '__main__':
    os.system('rm -rf __cache__/*')
    unittest.main()
