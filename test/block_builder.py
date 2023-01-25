import json
import os
import sys, unittest, json
from hashlib import sha3_256

sys.path.append('../src/')

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock, get_position_length
from grpcbigbuffer.utils import Enviroment
from grpcbigbuffer.utils import encode_bytes, create_lengths_tree


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
        h.value = sha3_256(b"block").digest()
        block.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir+sha3_256(b"block").hexdigest()):
            with open(Enviroment.block_dir+sha3_256(b"block").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block' for i in range(100)])
                )

        block2 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block2").digest()
        block2.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir+sha3_256(b"block2").hexdigest()):
            with open(Enviroment.block_dir+sha3_256(b"block2").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block2' for i in range(100)])
                )

        block3 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block3").digest()
        block3.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir+sha3_256(b"block3").hexdigest()):
            with open(Enviroment.block_dir+sha3_256(b"block3").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block3' for i in range(100)])
                )

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

        _object = Test()
        _object.t1 = b''.join([b'mc1' for i in range(100)])
        _object.t2 = b''.join([b'mc2' for i in range(100)])
        _object.t4.append(b)
        _object.t4.append(c)

        object_id, cache_dir = build_multiblock(
            pf_object_with_block_pointers=_object,
            blocks=[
                sha3_256(b"block").digest(),
                sha3_256(b"block2").digest(),
                sha3_256(b"block3").digest()
            ]
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
        print('\n\n')
        for element in _json:
            if type(element) == list:
                for _e in element[1]:
                    print(_e, '   ', get_position_length(_e, buffer), buffer[_e])
        print('\n\n')
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
        print('\n')
        for element in _json:
            if type(element) == list:
                for _e in element[1]:
                    print(
                        '\n\n',
                        str(_e) + ' ', get_position_length(_e, buffer),
                        encode_bytes(get_position_length(_e, buffer)),
                        buffer[_e:_e+get_position_length(_e, buffer)+len(encode_bytes(get_position_length(_e, buffer)))]
                    )


if __name__ == '__main__':
    os.system('rm -rf __cache__/*')
    unittest.main()
