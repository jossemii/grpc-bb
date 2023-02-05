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
    def test_filesystem(self):

        from grpcbigbuffer.test_pb2 import Filesystem, ItemBranch

        block1 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block1").digest()
        block1.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block1").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block1").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block1' for i in range(100)])
                )

        block2 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block2").digest()
        block2.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block2").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block2").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block2' for i in range(100)])
                )

        block3 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block3").digest()
        block3.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block3").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block3").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block3' for i in range(100)])
                )

        item1 = ItemBranch()
        item1.name = ''.join(['item1' for i in range(1)])
        item1.file = block1.SerializeToString()    
        
        item2 = ItemBranch()
        item2.name = ''.join(['item2' for i in range(100)])
        item2.file = block2.SerializeToString()    
        
        item3 = ItemBranch()
        item3.name = ''.join(['item3' for i in range(10)])
        item3.file = block3.SerializeToString()   
        
        item4 = ItemBranch()
        item4.name = "item4"
        item4.link = "item4" 
        
        item5 = ItemBranch()
        item5.name = "item5"
        item5.filesystem.branch.append(item2)
        item5.filesystem.branch.append(item4)

        filesystem: Filesystem = Filesystem()
        filesystem.branch.append(item1)
        filesystem.branch.append(item3)
        filesystem.branch.append(item5)

        object_id, cache_dir = build_multiblock(
            pf_object_with_block_pointers=filesystem,
            blocks=[
                sha3_256(b"block1").digest(),
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
                    block1 = f.read()
                    buffer += block1

            if type(element) == list:
                with open(os.path.join(Enviroment.block_dir, element[0]), 'rb') as f:
                    while True:
                        block1 = f.read(1024)

                        if not block1:
                            break
                        buffer += block1

        buff_object = Filesystem()
        buff_object.ParseFromString(buffer)

        def extract_last_elements(json_obj):
            result = []
            for _element in json_obj:
                if type(_element) == list and len(_element) == 2 and type(_element[0]) == str and type(
                        _element[1]) == list:
                    result.append(_element[1][-1])
            return result
        
        print('\n')
        for element in _json:
            if type(element) == list:
                for _e in element[1]:
                    print(
                        '\n\n',
                        str(_e) + ' ', get_position_length(_e, buffer),
                        encode_bytes(get_position_length(_e, buffer)),
                        buffer[
                            _e:_e + get_position_length(_e, buffer) + len(encode_bytes(get_position_length(_e, buffer)))
                        ],
                        buffer[_e:],
                        '\n'
                    )
                    
                    
                   
    def test_simple_filesystem(self):

        from grpcbigbuffer.test_pb2 import Filesystem, ItemBranch

        block1 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block1").digest()
        block1.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block1").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block1").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block1' for i in range(100)])
                )

        item1 = ItemBranch()
        item1.name = ''.join(['item1' for i in range(1)])
        item1.file = block1.SerializeToString()    
        
        filesystem: Filesystem = Filesystem()
        filesystem.branch.append(item1)
        
        object_id, cache_dir = build_multiblock(
            pf_object_with_block_pointers=filesystem,
            blocks=[
                sha3_256(b"block1").digest(),
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
                    block1 = f.read()
                    buffer += block1

            if type(element) == list:
                with open(os.path.join(Enviroment.block_dir, element[0]), 'rb') as f:
                    while True:
                        block1 = f.read(1024)

                        if not block1:
                            break
                        buffer += block1

        buff_object = Filesystem()
        buff_object.ParseFromString(buffer)

        def extract_last_elements(json_obj):
            result = []
            for _element in json_obj:
                if type(_element) == list and len(_element) == 2 and type(_element[0]) == str and type(
                        _element[1]) == list:
                    result.append(_element[1][-1])
            return result
    
        for element in _json:
            if type(element) == list:
                for _e in element[1]:
                    print(
                        '\n\n',
                        str(_e) + ' ', get_position_length(_e, buffer),
                        encode_bytes(get_position_length(_e, buffer)),
                        buffer[
                            _e:_e + get_position_length(_e, buffer) + len(encode_bytes(get_position_length(_e, buffer)))
                        ],
                        buffer[_e:],
                        '\n'
                    )


    def test_typical_complex_object(self):

        from grpcbigbuffer.test_pb2 import Test

        block1 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block1").digest()
        block1.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block1").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block1").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block1' for i in range(100)])
                )

        block2 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block2").digest()
        block2.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block2").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block2").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block2' for i in range(100)])
                )

        block3 = buffer_pb2.Buffer.Block()
        h = buffer_pb2.Buffer.Block.Hash()
        h.type = Enviroment.hash_type
        h.value = sha3_256(b"block3").digest()
        block3.hashes.append(h)

        if not os.path.isfile(Enviroment.block_dir + sha3_256(b"block3").hexdigest()):
            with open(Enviroment.block_dir + sha3_256(b"block3").hexdigest(), 'wb') as file:
                file.write(
                    b''.join([b'block3' for i in range(100)])
                )

        a = Test()
        a.t1 = b''.join([b'bt1' for i in range(1)])
        a.t2 = block1.SerializeToString()

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
        _object.t5 = b'final'

        object_id, cache_dir = build_multiblock(
            pf_object_with_block_pointers=_object,
            blocks=[
                sha3_256(b"block1").digest(),
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
        buff_object.ParseFromString(buffer)

        def extract_last_elements(json_obj):
            result = []
            for _element in json_obj:
                if type(_element) == list and len(_element) == 2 and type(_element[0]) == str and type(
                        _element[1]) == list:
                    result.append(_element[1][-1])
            return result

        for element in _json:
            if type(element) == list:
                for _e in element[1]:
                    print(
                        '\n\n',
                        str(_e) + ' ', get_position_length(_e, buffer),
                        encode_bytes(get_position_length(_e, buffer)),
                        buffer[_e:_e + get_position_length(_e, buffer) + len(
                            encode_bytes(get_position_length(_e, buffer)))]
                    )


if __name__ == '__main__':
    os.system('rm -rf __cache__/*')
    #unittest.main()
    TestBlockBuilder().test_simple_filesystem()