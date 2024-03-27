// This is a simplified translation of the Python code into Rust, focusing on the main structure and logic.
// It is not a direct line-by-line translation due to differences between Python and Rust.
// Some Python-specific constructs and libraries (e.g., gRPC, protobuf) do not have direct equivalents in Rust,
// so this translation assumes the existence of similar functionality in Rust libraries or crates.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::path::PathBuf;

use prost::Message;
use sha3::{Digest, Sha3_256};

// Assuming equivalent functionality is provided by a Rust gRPC library or crate.
use pee_rpc::{
    buffer_proto, client, utils, BufferBlock, Environment, CHUNK_SIZE, METADATA_FILE_NAME,
    WITHOUT_BLOCK_POINTERS_FILE_NAME,
};


// Assuming `Environment` and `CHUNK_SIZE` are defined elsewhere in the code.
// `Environment` should provide access to the `block_dir` path.
struct Environment;

impl Environment {
    fn block_dir() -> PathBuf {
        // Return the directory path where blocks are stored.
        // This should be defined based on your application's needs.
        "/path/to/block/dir/"
    }
}

// Placeholder types for the Protobuf message and hash types.
// These should be defined according to your actual Protobuf definitions.
struct MyProtobufMessage {
    // ...
}

impl MyProtobufMessage {
    fn list_fields(&self) -> Vec<(Field, &dyn Message)> {
        // Return a list of fields and their values for the message.
        // ...
        vec![]
    }
}

// Rust structs and enums to represent the necessary types and logic.
// ...

// Functions translated from Python to Rust.

fn is_block(bytes_obj: &[u8], blocks: &HashSet<Vec<u8>>) -> bool {
    let block = BufferBlock::decode(bytes_obj);
    match block {
        Ok(block) => {
            // Assuming get_hash_from_block returns a Vec<u8> for the hash.
            let hash = get_hash_from_block(&block, false, false);
            blocks.contains(&hash)
        }
        Err(_) => false,
    }
}

fn get_position_length(varint_pos: usize, buffer: &[u8]) -> Result<usize, &'static str> {
    /*
    Returns the value of the varint at the given position in the Protobuf buffer.
     */
    let mut value = 0usize;
    let mut shift = 0usize;
    let mut index = varint_pos;

    loop {
        // Check if the index is within the buffer bounds
        if index >= buffer.len() {
            return Err("Index out of bounds while decoding varint");
        }

        let byte = buffer[index] as usize;
        value |= (byte & 0x7F) << shift;

        if (byte & 0x80) == 0 {
            break;
        }

        shift += 7;
        index += 1;
    }

    Ok(value)
}

fn get_hash(block: &BufferBlock) -> Result<String, String> {
    let hash_type = Environment::hash_type() as i32; // Convert enum to i32.
    for hash in &block.hashes {
        if hash.hash_type == hash_type {
            // Convert the hash value to a hexadecimal string.
            return Ok(hex::encode(&hash.value));
        }
    }
    Err(format!(
        "gRPCbb: any hash of type {:?} not found",
        Environment::hash_type()
    ))
}


fn get_block_length(block_id: &str) -> Result<usize, String> {
    let block_path = Environment::block_dir().join(block_id);

    if block_path.is_file() {
        // Return the file size in bytes.
        fs::metadata(&block_path)
            .map_err(|e| e.to_string())
            .map(|metadata| metadata.len() as usize)
    } else if block_path.is_dir() {
        Err(format!(
            "gRPCbb: error on compute_real_lengths, multiblock blocks not supported. {}",
            block_path.display()
        ))
    } else {
        Err(format!(
            "gRPCbb: error on compute_real_lengths, block does not exist in block registry. {}",
            block_path.display()
        ))
    }
}

fn search_on_message_real(
    message: &dyn Message,
    pointers: Vec<usize>,
    initial_position: usize,
    real_initial_position: usize,
    blocks: &[Vec<u8>],
    container: &mut Vec<(String, Vec<usize>)>,
    real_lengths: &HashMap<usize, (usize, usize, bool)>,
) {
    let mut position = initial_position;
    let mut real_position = real_initial_position;
    for (field, value) in message.list_fields() {
        match value {
            // Match against the specific types of fields in your Protobuf message.
            // Assuming `RepeatedCompositeContainer` is a placeholder for a repeated field.
            RepeatedCompositeContainer(elements) => {
                for element in elements {
                    position += 1;
                    if !real_lengths.contains_key(&position) {
                        position += encode_bytes(element.encoded_len()).len() + element.encoded_len();
                        real_position += 1 + encode_bytes(element.encoded_len()).len() + element.encoded_len();
                        continue;
                    }
                    let message_size = real_lengths[&position].0;
                    search_on_message_real(
                        element,
                        [&pointers[..], &[real_position + 1]].concat(),
                        position + encode_bytes(element.encoded_len()).len(),
                        real_position + 1 + encode_bytes(message_size).len(),
                        blocks,
                        container,
                        real_lengths,
                    );
                    position += encode_bytes(element.encoded_len()).len() + element.encoded_len();
                    real_position += 1 + encode_bytes(message_size).len() + message_size;
                }
            }
            // Assuming `Message` is a placeholder for a nested Protobuf message.
            Message(nested_message) => {
                position += 1;
                if !real_lengths.contains_key(&position) {
                    position += encode_bytes(nested_message.encoded_len()).len() + nested_message.encoded_len();
                    real_position += 1 + encode_bytes(nested_message.encoded_len()).len() + nested_message.encoded_len();
                    continue;
                }
                let message_size = real_lengths[&position].0;
                search_on_message_real(
                    nested_message,
                    [&pointers[..], &[real_position + 1]].concat(),
                    position + encode_bytes(nested_message.encoded_len()).len(),
                    real_position + 1 + encode_bytes(message_size).len(),
                    blocks,
                    container,
                    real_lengths,
                );
                position += encode_bytes(nested_message.encoded_len()).len() + nested_message.encoded_len();
                real_position += 1 + encode_bytes(message_size).len() + message_size;
            }
            // For bytes fields, check if it's a block.
            Bytes(bytes_value) if is_block(bytes_value, blocks) => {
                let mut block = MyProtobufMessage::default();
                block.merge(bytes_value).expect("Failed to parse block");

                let block_hash = get_hash(&block);
                container.push((block_hash, [&pointers[..], &[real_position + 1]].concat()));

                let block_length = get_block_length(&block_hash);
                position += 1 + encode_bytes(block.encoded_len()).len() + block.encoded_len();
                real_position += 1 + encode_bytes(block_length).len() + block_length;
            }
            // For other bytes or string fields, increment position accordingly.
            Bytes(bytes_value) | String(string_value) => {
                position += 1 + encode_bytes(string_value.len()).len() + string_value.len();
                real_position += 1 + encode_bytes(string_value.len()).len() + string_value.len();
            }
            // For any other field types, handle them accordingly.
            _ => {
                // Handle other field types or raise an error.
            }
        }
    }
}

fn search_on_message(
    message: &dyn Message,
    pointers: Vec<usize>,
    initial_position: usize,
    blocks: &[Vec<u8>],
    container: &mut HashMap<String, Vec<Vec<usize>>>,
) {
    /*
        Search_on_message makes a tree search of the protobuf object (attr. message) and stores all the buffer block
        identifier instances with its indexes (ascendant order) on the container dictionary.
        It allows to know where the buffer needs to be changed when the buffer block substitute the block identifier.
     */
    let mut position = initial_position;
    for (field, value) in message.list_fields() {
        match value {
            // Match against the specific types of fields in your Protobuf message.
            // Assuming `RepeatedCompositeContainer` is a placeholder for a repeated field.
            RepeatedCompositeContainer(elements) => {
                for element in elements {
                    search_on_message(
                        element,
                        [&pointers[..], &[position + 1]].concat(),
                        position + 1 + encode_bytes(element.encoded_len()).len(),
                        blocks,
                        container,
                    );
                    position += 1 + encode_bytes(element.encoded_len()).len() + element.encoded_len();
                }
            }
            // Assuming `Message` is a placeholder for a nested Protobuf message.
            Message(nested_message) => {
                search_on_message(
                    nested_message,
                    [&pointers[..], &[position + 1]].concat(),
                    position + 1 + encode_bytes(nested_message.encoded_len()).len(),
                    blocks,
                    container,
                );
                position += 1 + encode_bytes(nested_message.encoded_len()).len() + nested_message.encoded_len();
            }
            // For bytes fields, check if it's a block.
            Bytes(bytes_value) if is_block(bytes_value, blocks) => {
                let mut block = MyProtobufMessage::default();
                block.merge(bytes_value).expect("Failed to parse block");

                let block_hash = get_hash(&block);
                let list_of_pointers = [&pointers[..], &[position + 1]].concat();
                container.entry(block_hash).or_insert_with(Vec::new).push(list_of_pointers);

                position += 1 + encode_bytes(block.encoded_len()).len() + block.encoded_len();
            }
            // For other bytes or string fields, increment position accordingly.
            Bytes(bytes_value) | String(string_value) => {
                position += 1 + encode_bytes(string_value.len()).len() + string_value.len();
            }
            // For any other field types, handle them accordingly.
            _ => {
                // Handle other field types or raise an error.
            }
        }
    }
}

fn encode_bytes(mut n: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        let mut towrite = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            towrite |= 0x80;
        }
        buf.push(towrite);
        if n == 0 {
            break;
        }
    }
    buf
}

fn compute_real_lengths(
    tree: &HashMap<usize, Either<HashMap<usize, String>, String>>,
    buffer: &[u8],
) -> Result<HashMap<usize, (usize, usize, bool)>, String> {
    /*
    Given the pointer's tree with block id's as the leafs it will return a dict of pointers with its
    real length, compressed format (of the input buffer) length, and a boolean saying if it's the pointer
    of a block id message or not (tree's leaf or not).

    :param tree:Tree of pointers as nodes and block id's as leafs.
    :type tree: Dict[int, Union[Dict, str]]
    :param buffer:Buffer of the compressed object.
    :type buffer: bytes
    :return: A dict of pointers with its real and compressed lengths and if it's leaf or not.
    :rtype: Dict[int, Tuple[int, int, bool]]
     */
    fn traverse_tree(
        internal_tree: &HashMap<usize, Either<HashMap<usize, String>, String>>,
        internal_buffer: &[u8],
        initial_total_length: usize,
    ) -> Result<(usize, HashMap<usize, (usize, usize, bool)>), String> {
        let mut real_lengths = HashMap::new();
        let mut total_tree_length = 0;
        let mut total_block_length = 0;

        for (&key, value) in internal_tree {
            match value {
                Either::Left(nested_tree) => {
                    let initial_length = get_position_length(key, internal_buffer);
                    let (real_length, internal_lengths) = traverse_tree(nested_tree, internal_buffer, initial_length)?;
                    real_lengths.insert(key, (real_length, initial_length, false));
                    real_lengths.extend(internal_lengths);
                    total_tree_length += real_length + encode_bytes(real_length).len() + 1;
                    total_block_length += initial_length + encode_bytes(initial_length).len() + 1;
                },
                Either::Right(block_id) => {
                    let mut block = BufferBlock {
                        // ...
                    };
                    // Assume hash_type() provides the correct hash type.
                    let hash_type = Environment::hash_type();
                    let value_bytes = hex::decode(block_id).map_err(|e| e.to_string())?;
                    // ... Set up the hash and block here ...
                    let b_length = block.serialize_to_vec().len();
                    let real_length = get_block_length(block_id);
                    real_lengths.insert(key, (real_length, b_length, true));
                    total_tree_length += real_length + encode_bytes(real_length).len() + 1;
                    total_block_length += b_length + encode_bytes(b_length).len() + 1;
                },
            }
        }

        if initial_total_length < total_block_length {
            return Err("Error on compute real lengths, block length can't be greater than the total length".to_string());
        }

        total_tree_length += initial_total_length - total_block_length;
        Ok((total_tree_length, real_lengths))
    }

    let (_, real_lengths) = traverse_tree(tree, buffer, buffer.len())?;
    Ok(real_lengths.into_iter().collect())
}

fn generate_buffer(buffer: &[u8], lengths: &HashMap<usize, (usize, usize, bool)>) -> Vec<Vec<u8>> {
    /*
    Iterates over the buffer, replacing the compressed buffer with the real buffer,
    replacing the compressed lengths of each pointer with its real length.
    It is returned in list format since it is not necessary to return the entire buffer,
    the content of the blocks does not need to be loaded into memory.

    :param buffer: Compressed buffer
    :type buffer: bytes
    :param lengths:  A dict of pointers with its real and compressed lengths and if it's leaf or not.
    :type lengths: Dict[int, Tuple[int, int, bool]]
    :return: The inter-block buffers list with the real lengths.
    :rtype: List[bytes]
    */
    let mut list_of_bytes: Vec<Vec<u8>> = Vec::new();
    let mut new_buff: Vec<u8> = Vec::new();
    let mut i: usize = 0;
    for (&key, &(real_length, compressed_length, is_leaf)) in lengths {
        new_buff.extend(&buffer[i..key]);
        new_buff.extend(encode_bytes(real_length));
        i = key + encode_bytes(compressed_length).len();
        if is_leaf {
            i += compressed_length;
            list_of_bytes.push(new_buff.clone());
            new_buff.clear();
        }
    }
    list_of_bytes.push(buffer[i..].to_vec());
    list_of_bytes
}

fn generate_id(buffers: &[Vec<u8>], blocks: &[Vec<u8>]) -> io::Result<Vec<u8>> {
    let mut hash_id = Sha3_256::new();
    for (buffer, block) in buffers.iter().zip_longest(blocks.iter()) {
        if let Some(buf) = buffer {
            hash_id.update(buf);
        }
        if let Some(block) = block {
            let block_path = format!("{}{}", Environment::block_dir(), hex::encode(block));
            let mut file = BufReader::new(File::open(block_path)?);
            let mut piece = vec![0u8; CHUNK_SIZE];
            loop {
                let bytes_read = file.read(&mut piece)?;
                if bytes_read == 0 {
                    break;
                }
                hash_id.update(&piece[..bytes_read]);
            }
        }
    }
    Ok(hash_id.finalize().to_vec())
}

fn purify_buffer(buff: &[u8]) -> Vec<u8> {
    buff
}


// The protobuf message type, as well as the specific hash and block types, will need to be defined
// according to the .proto files used in your project. For this example, we will use placeholder types.
struct ProtobufMessageWithBlockPointers; // Placeholder for the actual Protobuf message type.

impl ProtobufMessageWithBlockPointers {
    fn serialize_to_vec(&self) -> Vec<u8> {
        // Serialize the message to a byte vector.
        // This should be implemented using prost or another Protobuf library.
        vec![]
    }
}

fn build_multiblock(
    pf_object_with_block_pointers: ProtobufMessageWithBlockPointers,
    blocks: Vec<Vec<u8>>,
) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
    let mut container = HashMap::new();
    search_on_message(
        &pf_object_with_block_pointers,
        &mut vec![],
        0,
        &blocks,
        &mut container,
    );

    let tree = create_lengths_tree(&container);
    let buffer = pf_object_with_block_pointers.serialize_to_vec();
    let real_lengths = compute_real_lengths(&tree, &buffer);
    let new_buff = generate_buffer(&buffer, &real_lengths);
    let object_id = generate_id(&new_buff, &blocks);
    let cache_dir = generate_random_dir() + "/";
    let mut _json = vec![];

    let mut container_real_lengths = vec![];
    search_on_message_real(
        &pf_object_with_block_pointers,
        &mut vec![],
        0,
        0,
        &blocks,
        &mut container_real_lengths,
        &real_lengths,
    );

    for (i, (b1, b2)) in new_buff.into_iter().zip_longest(container_real_lengths).enumerate() {
        _json.push(i + 1);
        let mut file = File::create(PathBuf::from(&cache_dir).join(i.to_string()))?;
        file.write_all(&b1)?;

        if let Some(b2) = b2 {
            _json.push((b2.0, b2.1));
        }
    }

    let metadata_file_path = PathBuf::from(&cache_dir).join(METADATA_FILE_NAME);
    let mut metadata_file = File::create(metadata_file_path)?;
    metadata_file.write_all(serde_json::to_string(&_json)?.as_bytes())?;

    let without_block_pointers_file_path = PathBuf::from(&cache_dir).join(WITHOUT_BLOCK_POINTERS_FILE_NAME);
    let mut without_block_pointers_file = File::create(without_block_pointers_file_path)?;
    without_block_pointers_file.write_all(&pf_object_with_block_pointers.serialize_to_vec())?;

    Ok((object_id, cache_dir))
}

fn create_block(file_path: &Path, copy: bool) -> io::Result<(Vec<u8>, BufferBlock)> {
    let file_hash_str = get_file_hash(file_path)?;

    if !block_exists(&file_hash_str) {
        if copy {
            copy_to_block_dir(&file_hash_str, file_path)?;
        } else {
            move_to_block_dir(&file_hash_str, file_path)?;
        }
    }

    let file_hash_bytes = hex::decode(file_hash_str).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut block = BufferBlock::default();
    block.hashes.push(HashType {
        hash_type: Environment::hash_type(), // Assuming Environment is a struct that provides the hash type.
        value: file_hash_bytes.clone(),
    });

    Ok((file_hash_bytes, block))
}



#[cfg(test)]
mod tests {
    use super::*; // Import the functions from the parent module (or specify the path to your functions)

    #[test]
    fn test_create_lengths_tree_single_element() {
        // Test with a single element
        let pointer_container = vec![("abc".to_string(), vec![1, 2, 3])].into_iter().collect();
        let expected_output: HashMap<usize, Either<HashMap<usize, String>, String>> = 
            [(1, Either::Left([(2, Either::Left([(3, Either::Right("abc".to_string()))].into_iter().collect()))].into_iter().collect()))].into_iter().collect();
        assert_eq!(create_lengths_tree(&pointer_container), expected_output);
    }

    #[test]
    fn test_create_lengths_tree_multiple_elements() {
        // Test with multiple elements
        let pointer_container = vec![
            ("abc".to_string(), vec![1, 2, 3, 5]),
            ("abc".to_string(), vec![1, 16]),
            ("fjk".to_string(), vec![1, 8]),
        ].into_iter().collect();
        let expected_output: HashMap<usize, Either<HashMap<usize, String>, String>> = 
            [(1, Either::Left([
                (2, Either::Left([
                    (3, Either::Left([
                        (5, Either::Right("abc".to_string()))
                    ].into_iter().collect()))
                ].into_iter().collect())),
                (8, Either::Right("fjk".to_string())),
                (16, Either::Right("abc".to_string()))
            ].into_iter().collect()))].into_iter().collect();
        assert_eq!(create_lengths_tree(&pointer_container), expected_output);
    }

    #[test]
    fn test_create_lengths_tree_empty_input() {
        // Test with empty input
        let pointer_container: HashMap<String, Vec<usize>> = HashMap::new();
        let expected_output: HashMap<usize, Either<HashMap<usize, String>, String>> = HashMap::new();
        assert_eq!(create_lengths_tree(&pointer_container), expected_output);
    }
}