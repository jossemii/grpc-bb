
# ANSI escape codes for text colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def extract_protobuf_data(binary_data):
    result = []

    # Iterate through the binary data to extract messages
    while binary_data:
        # Decode the field tag and wire type from the first byte
        field_tag = binary_data[0]
        field_number = field_tag >> 3
        wire_type = field_tag & 0x07

        # Handle length-delimited fields (wire type 2)
        if wire_type == 2:
            # Decode the length (varint decoding)
            length = 0
            shift = 0
            for i in range(1, len(binary_data)):
                byte = binary_data[i]
                length |= (byte & 0x7F) << shift
                shift += 7
                if not byte & 0x80:
                    binary_data = binary_data[i + 1:]
                    break
            else:
                raise ValueError("Malformed varint encoding")

            # Check if the length is equal to the binary_data length
            if len(binary_data) != length:
                result.append((field_number, length, binary_data))
                break

            # Read the message based on the length
            message = binary_data[:length]

            binary_data = binary_data[length:]

            # Append the extracted data to the result
            result.append((field_number, length, message))

        else:  # If it's not a protobuf message.
            break

    return result


def analyze_protobuf_data(binary_data, _tab=""):
    for field_number, length, message in extract_protobuf_data(binary_data=binary_data):
        # Assuming the message is UTF-8 encoded
        decoded_message = message.decode('utf-8')
        if length == len(message):
            print(f"\n\n"
                  f"{_tab} Field Number: {field_number}\n"
                  f"{_tab} Length: {length}\n"
                  f"{_tab} Message: {message}\n"
                  f"{_tab} {BLUE}Decoded message: {decoded_message}{RESET}\n"
                  f"{_tab} {GREEN}Length matches message length.{RESET}\n")

            analyze_protobuf_data(binary_data=message, _tab=_tab+"  ")

        else:
            print(f"\n\n"
                  f"{_tab} Field Number: {field_number}\n"
                  f"{_tab} Length: {length}\n"
                  f"{_tab} Real Length: {len(decoded_message)}\n"
                  f"{_tab} Message: {message}\n"
                  f"{_tab} {BLUE}Decoded message: {decoded_message}{RESET}\n"
                  f"{_tab} {RED}Length does not match message length.{RESET}\n")


# Example usage:
if __name__ == "__main__":
    analyze_protobuf_data(
        binary_data=b'\n\x09\n\x07item444'
    )
