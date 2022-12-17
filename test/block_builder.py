import sys, unittest

sys.path.append('../src/')

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock


class MyTestCase(unittest.TestCase):
    def test_something(self):
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

        print(
            build_multiblock(
                pf_object_with_block_pointers=c,
                blocks=[b'sha256', b'sha512', b'sha3256']
            )
        )
        print(c.SerializeToString())
        print('\n')

        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
