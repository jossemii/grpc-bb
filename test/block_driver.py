import os
import sys, unittest

sys.path.append('../src/')
from grpcbigbuffer.block_driver import get_position_length

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_builder import build_multiblock
from grpcbigbuffer.client import Enviroment


class TestGetVarintValue(unittest.TestCase):
    def test_simple_varint(self):
        # Test a simple varint with a single byte
        buffer = b"\x7F"
        value = get_position_length(0, buffer)
        self.assertEqual(value, 127)

    def test_multi_byte_varint(self):
        # Test a varint with multiple bytes
        buffer = b"\xC0\x03"
        value = get_position_length(0, buffer)
        self.assertEqual(value, 448)

    def test_max_varint(self):
        # Test the maximum varint value (2**64 - 1)
        buffer = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"
        value = get_position_length(0, buffer)
        self.assertEqual(value, 18446744073709551615)

    def test_varint_at_position(self):
        # Test a varint at a non-zero position in the buffer
        buffer = b"\x00\x00\xC0\x03"
        value = get_position_length(2, buffer)
        self.assertEqual(value, 448)


    def test_generate_wbp_file(self):
        # Assuming that the build_multiblock_directory() function works correctly (tests/block_builder.py is OK)
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

        # Test generate_wbp_file
        from grpcbigbuffer.block_driver import generate_wbp_file
        os.system('rm '+cache_dir+'/wbp.bin')
        generate_wbp_file(cache_dir)

        generated = Test()
        generated.ParseFromString(
            open(cache_dir+'/wbp.bin', 'rb').read()
        )

        self.assertEqual(
            ultra_complex,
            generated
        )


if __name__ == "__main__":
    os.system('rm -rf __cache__/*')
    unittest.main()
