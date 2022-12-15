import sys, unittest

sys.path.append('../src/')
from grpcbigbuffer.block_driver import get_position_length


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


if __name__ == "__main__":
    unittest.main()
