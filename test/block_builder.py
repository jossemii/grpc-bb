import json
import os
import sys, unittest


sys.path.append('../src/')

from grpcbigbuffer.client import Enviroment
from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock, create_lengths_tree
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

        b = Test()
        b.t1 = block2.SerializeToString()
        b.t2 = block.SerializeToString()

        c = Test()
        c.t1 = block3.SerializeToString()
        c.t2 = b''.join([b'ja' for i in range(100)])
        c.t3.CopyFrom(b)

        more_complex = Test()
        more_complex.t1 = b''.join([b'ho' for i in range(100)])
        more_complex.t3.CopyFrom(c)

        more_more_complex = Test()
        more_more_complex.t1 = b''.join([b'la' for i in range(100)])
        more_more_complex.t2 = b''.join([b'abc' for i in range(100)])
        more_more_complex.t3.CopyFrom(more_complex)
        more_more_complex.t4.append(b)
        more_more_complex.t4.append(c)

        ultra_complex = Test()
        ultra_complex.t1 = b''.join([b'jo' for i in range(100)])
        ultra_complex.t2 = b''.join([b'hi' for i in range(100)])
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

        object = Test()
        object.ParseFromString(buffer)

        correct_buffer: bytes = b'\n\xc8\x01jojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojojo\x12\xc8\x01hihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihihi\x1a\xdc\x0b\n\xc8\x01lalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalala\x12\xac\x02abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc\x1a\xa6\x04\n\xc8\x01hohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohoho\x1a\xd8\x02\n-\n+\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x07sha3256\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\xd8\x02\n-\n+\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x07sha3256\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\xd8\x02\n-\n+\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x07sha3256\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\xa6\x04\n\xc8\x01hohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohoho\x1a\xd8\x02\n-\n+\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x07sha3256\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\x97\r\n\xc8\x01lalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalalala\x12\xac\x02abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc\x1a\xa6\x04\n\xc8\x01hohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohoho\x1a\xd8\x02\n-\n+\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x07sha3256\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\\\n,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha256\x12,\n*\n \xa7\xff\xc6\xf8\xbf\x1e\xd7fQ\xc1GV\xa0a\xd6b\xf5\x80\xffM\xe4;I\xfa\x82\xd8\nK\x80\xf8CJ\x12\x06sha512"\x93\x04\n\x8a\x01sha3256llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll\n\x12\xc8\x01jajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajajaja\x1a\xb8\x01\nJsha256ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt\n\x12jsha512aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'

        self.assertEqual(
            buffer,
            correct_buffer
        )


if __name__ == '__main__':
    unittest.main()

