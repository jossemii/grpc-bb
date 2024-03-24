# Tests Directory

This directory contains a series of test scripts designed to ensure the reliability and correctness of the project modules. Below is a description of each test script along with its purpose.

## Test Scripts

### `analyze_protobuf_data.py`

This test script is responsible for verifying the integrity of protobuf data. It checks if the headers of the buffers accurately report the length of their respective buffers. This is crucial for ensuring that the data serialized using protobuf is consistent and can be correctly deserialized without errors.

Usage:
```bash
python test/analyze_protobuf_data.py
```

### `block_builder.py`

This script tests the functionality of the block_builder.py module. It ensures that the block builder component of the project is functioning as expected, creating data blocks correctly and handling all specified cases.

Usage:

```bash
python test/block_builder.py
```

### `block_driver.py`

The purpose of this script is to test the block_driver.py module. It checks the module's ability to interface with the underlying system or framework to drive the data blocks through the necessary processes.

Usage:

```bash
python test/block_driver.py
```
