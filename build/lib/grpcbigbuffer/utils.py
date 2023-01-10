import hashlib


def get_file_hash(file_path: str) -> str:
    # Create a hash object
    hash = hashlib.sha3_256()
    # Open the file in binary mode
    with open(file_path, 'rb') as file:
        # Read the contents of the file in chunks
        chunk = file.read(1024)
        while chunk:
            # Update the hash with the chunk
            hash.update(chunk)
            # Read the next chunk
            chunk = file.read(1024)
        # Calculate the final hash
        file_hash: str = hash.hexdigest()
        # Return the hash
        return file_hash
