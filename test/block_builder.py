import sys, unittest

sys.path.append('../src/')

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock, create_lengths_tree, search_on_message


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
        pointer_container = {'abc': [1,2,3]}
        expected_output = {1: {2: { 3: 'abc'}}}
        self.assertEqual(create_lengths_tree(pointer_container), expected_output)

        # Test with multiple elements
        pointer_container = {'abc': [1,2,3, 5], 'fjk': [1,8]}
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

    build_multiblock(
        pf_object_with_block_pointers=c,
        blocks = [b'sha256', b'sha512', b'sha3256']
    )