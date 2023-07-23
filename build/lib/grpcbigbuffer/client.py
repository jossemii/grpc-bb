import inspect
import itertools
import json
import os
import shutil
import sys
import typing
import warnings
from random import randint
from typing import Generator, Union, List, Dict

from google import protobuf
from google.protobuf.message import DecodeError, Message
from google._upb._message import RepeatedCompositeContainer

from grpcbigbuffer import buffer_pb2
from grpcbigbuffer.block_driver import generate_wbp_file, WITHOUT_BLOCK_POINTERS_FILE_NAME, METADATA_FILE_NAME
from grpcbigbuffer.reader import read_block, read_multiblock_directory, read_from_registry, block_exists
from grpcbigbuffer.utils import Enviroment, MAX_DIR, Signal, EmptyBufferException, Dir, CHUNK_SIZE


## Block driver ##
def contain_blocks(message: Message) -> bool:
    for field, value in message.ListFields():
        if isinstance(value, RepeatedCompositeContainer):
            for element in value:
                if contain_blocks(element):
                    return True

        elif isinstance(value, Message) and contain_blocks(value):
            return True

        elif type(value) == bytes:
            try:
                block = buffer_pb2.Buffer.Block()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    block.ParseFromString(value)
                    return True
            except DecodeError:
                pass

    return False


def copy_block_if_exists(buffer: bytes, directory: str) -> bool:
    # TODO support copy of multiblocks blocks. Now it will create a single file block.
    try:
        block = buffer_pb2.Buffer.Block()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            block.ParseFromString(buffer)
    except DecodeError:
        return False

    block_id: typing.Optional[str] = get_hash_from_block(block=block, internal_block=True)
    if not block_id:
        return False

    try:
        with open(directory, 'wb') as file:
            for data in read_block(
                    block_id=block_id
            ):
                file.write(data)
        return True
    except Exception as e:  # TODO control only Exception('gRPCbb: Error reading block.')
        return False


def move_to_block_dir(file_hash: str, file_path: str) -> bool:
    if not block_exists(block_id=file_hash) and os.path.isfile(file_path):
        try:
            # Use a filesystem-specific method to move the file without reading or writing the contents
            # (e.g. link() and unlink() on Unix-like systems) for improved performance.
            destination_path = os.path.join(Enviroment.block_dir, file_hash)
            os.rename(file_path, destination_path)
            return True
        except Exception as e:
            raise Exception('gRPCbb error creating block, file could not be moved: ' + str(e))
    return False


def copy_to_block_dir(file_hash: str, file_path: str) -> bool:
    if not block_exists(block_id=file_hash) and os.path.isfile(file_path):
        try:
            destination_path = os.path.join(Enviroment.block_dir, file_hash)
            shutil.copyfile(file_path, destination_path)
            return True
        except Exception as e:
            raise Exception('gRPCbb error creating block, file could not be moved: ' + str(e))
    return False


def signal_block_buffer_stream(hash: str):
    # Receiver sends the Buffer with block attr. for stops the block buffer stream.
    pass  # Sends Buffer(block=Block())


def get_hash_from_block(block: buffer_pb2.Buffer.Block,
                        internal_block: bool = False,
                        hexadecimal: bool = True
                        ) -> typing.Optional[str]:
    if internal_block:
        if len(block.hashes) == 1 and block.hashes[0].type == b'':
            return block.hashes[0].value.hex() if hexadecimal else block.hashes[0].value
    else:
        for hash in block.hashes:
            if hash.type == Enviroment.hash_type:
                return hash.value.hex() if hexadecimal else hash.value
    return None


def generate_random_dir() -> str:
    cache_dir = Enviroment.cache_dir
    try:
        os.mkdir(cache_dir)
    except FileExistsError:
        pass
    while True:
        try:
            new_dir: str = cache_dir + str(randint(1, MAX_DIR))
            os.mkdir(new_dir)
            return new_dir
        except FileExistsError:
            pass


def generate_random_file() -> str:
    cache_dir = Enviroment.cache_dir
    try:
        os.mkdir(cache_dir)
    except FileExistsError:
        pass
    while True:
        file = cache_dir + str(randint(1, MAX_DIR))
        if not os.path.isfile(file): return file


def message_to_bytes(message) -> bytes:
    if inspect.isclass(type(message)) and issubclass(type(message), Message):
        return message.SerializeToString()
    elif type(message) is str:
        return bytes(message, 'utf-8')
    else:
        try:
            return bytes(message)
        except TypeError:
            raise (
                    'gRPCbb error -> Serialize message error: some primitive type message not suported for contain partition ' + str(
                type(message)))


def remove_file(file: str):
    os.remove(file)  # TODO could be async.


def remove_dir(dir: str):
    shutil.rmtree(dir)


def i_read_multiblock_directory(directory: str, delete_directory: bool = False, ignore_blocks: bool = True) \
        -> Generator[Union[bytes, buffer_pb2.Buffer.Block], None, None]:
    for i in read_multiblock_directory(directory, delete_directory, ignore_blocks):
        yield i


def stop_generator(iterator, block_id):
    for b in iterator:
        if b.HasField('block') and get_hash_from_block(b.block) == block_id:
            b.ClearField('block')
            yield b
            break
        else:
            yield b


def save_chunks_to_block(
        block_buffer: buffer_pb2.Buffer,
        buffer_iterator,
        signal: Signal = None,
        _json: List[Union[
            int,
            typing.Tuple[str, List[int]]
        ]] = None
):
    block_id: str = get_hash_from_block(block_buffer.block)
    if _json:
        _json.append(
            (block_id, list(block_buffer.block.previous_lengths_position))
        )
    if not block_exists(block_id):  # Second com probation of that.
        save_chunks_to_file(
            prev=block_buffer.chunk if block_buffer.HasField('chunk') else None,
            buffer_iterator=stop_generator(buffer_iterator, block_id),
            filename=Enviroment.block_dir + block_id,
            signal=signal
        )
    else:
        for buffer in buffer_iterator:
            if buffer.HasField('block') and \
                    get_hash_from_block(buffer.block) == block_id:
                break


def save_chunks_to_file(
        buffer_iterator,
        filename: str,
        signal: Signal = None,
        _json: List[Union[
            int,
            typing.Tuple[str, List[int]]
        ]] = None,
        prev: typing.Optional[bytes] = None
) -> bool:
    if not signal: signal = Signal(exist=False)
    signal.wait()
    with open(filename, 'wb') as f:
        signal.wait()
        if prev:
            f.write(prev)
            del prev

        for buffer in buffer_iterator:
            if buffer.HasField('block'):
                save_chunks_to_block(
                    block_buffer=buffer,
                    buffer_iterator=buffer_iterator,
                    signal=signal,
                    _json=_json
                )
                return False
            f.write(buffer.chunk)
        return True


def get_subclass(partition, object_cls):
    return get_subclass(
        object_cls=type(
            getattr(
                object_cls(),
                object_cls.DESCRIPTOR.fields_by_number[list(partition.index.keys())[0]].name
            )
        ),
        partition=list(partition.index.values())[0]
    ) if len(partition.index) == 1 else object_cls


def copy_message(obj, field_name, message):  # TODO for list too.
    e = getattr(obj, field_name) if field_name else obj
    if hasattr(message, 'CopyFrom'):
        e.CopyFrom(message)
    elif type(message) is bytes:
        e.ParseFromString(message)
    else:
        e = message
    return obj


def get_submessage(partition, obj, say_if_not_change=False):
    if len(partition.index) == 0:
        return False if say_if_not_change else obj
    if len(partition.index) == 1:
        for field in obj.DESCRIPTOR.fields:
            if field.index + 1 not in partition.index:
                obj.ClearField(field.name)
        return get_submessage(
            partition=list(partition.index.values())[0],
            obj=getattr(obj, obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name)
        )
    for field in obj.DESCRIPTOR.fields:
        if field.index + 1 in partition.index:
            try:
                submessage = get_submessage(
                    partition=partition.index[field.index + 1],
                    obj=getattr(obj, field.name),
                    say_if_not_change=True
                )
                if not submessage: continue  # Anything to prune.
                copy_message(
                    obj=obj, field_name=field.name,
                    message=submessage
                )
            except:
                pass
        else:
            obj.ClearField(field.name)
    return obj


def put_submessage(partition, message, obj):
    if len(partition.index) == 0:
        return copy_message(
            obj=obj, field_name=None,
            message=message
        )
    if len(partition.index) == 1:
        p = list(partition.index.values())[0]
        if len(p.index) == 1:
            field_name = obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name
            return copy_message(
                obj=obj, field_name=field_name,
                message=put_submessage(
                    partition=p,
                    obj=getattr(obj, field_name),
                    message=message,
                )
            )
        else:
            return copy_message(
                obj=obj, field_name=obj.DESCRIPTOR.fields[list(partition.index.keys())[0] - 1].name,
                message=message
            )


def combine_partitions(
        obj_cls: Message,
        partitions_model: tuple,
        partitions: typing.Tuple[str]
):
    obj = obj_cls()
    for i, partition in enumerate(partitions):
        if type(partition) is str and os.path.isfile(partition):
            with open(partition, 'rb') as f:
                partition: bytes = f.read()
        elif type(partition) is str and os.path.isdir(partition):
            with open(partition + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME, 'rb') as f:
                partition: bytes = f.read()
        elif not (hasattr(partition, 'SerializeToString') or not type(
                partition) is bytes):  # TODO check.   'not type(partition) is bytes' could affect on partitions to buffer()
            raise Exception('Partitions to buffer error.')
        obj = put_submessage(
            partition=partitions_model[i],
            message=partition,
            obj=obj
        )
    return obj


def parse_from_buffer(
        request_iterator,
        signal: Signal = None,
        indices: Union[Message, Dict[int, Message]] = None,
        partitions_message_mode: Union[bool, Dict[int, bool]] = False,  # Write on disk by default.
        mem_manager=None,
):
    try:
        if not indices:
            indices = buffer_pb2.Empty()
        if not signal:
            signal = Signal(exist=False)
        if not mem_manager:
            mem_manager = Enviroment.mem_manager
        if type(indices) is not dict:
            if issubclass(indices, Message):
                indices = {1: indices}
            else:
                raise Exception

        indices.update({0: bytes})
        if type(partitions_message_mode) is bool:
            partitions_message_mode = {i: partitions_message_mode for i in indices}
        elif type(partitions_message_mode) is dict:
            partitions_message_mode.update(
                {i: [False] for i in indices if i not in partitions_message_mode})  # Check that it've all indices.
        else:
            raise Exception("Incorrect partitions message mode type on parse_from_buffer.")

        if partitions_message_mode.keys() != indices.keys():
            raise Exception("Partitions message mode keys != indices keys on parse_from_buffer")

    except Exception as e:
        raise Exception(f'Parse from buffer error: Partitions or Indices are not correct. '
                        f'{partitions_message_mode} - {indices} - {str(e)}')

    def parser_iterator(
            request_iterator_obj,
            signal_obj: Signal = None,
            blocks: List[str] = None
    ) -> Generator[buffer_pb2.Buffer, None, None]:
        if not signal_obj: signal_obj = Signal(exist=False)
        _break: bool = True
        while _break:
            try:
                buffer_obj = next(request_iterator_obj)
            except StopIteration:
                raise Exception('AbortedIteration')

            if buffer_obj.HasField('signal') and buffer_obj.signal:
                signal_obj.change()

            if not blocks and buffer_obj.HasField('block') or \
                    blocks and buffer_obj.HasField('block') and len(blocks) < Enviroment.block_depth:

                block_hash: str = get_hash_from_block(buffer_obj.block)
                if block_hash:
                    if blocks and block_hash in blocks:
                        if blocks.pop() == block_hash:
                            break
                        else:
                            raise Exception('gRPCbb: IntersectionError: Intersections between blocks are not allowed.')

                    else:
                        if not blocks:
                            blocks = [block_hash]
                        else:
                            blocks.append(block_hash)

                        if block_exists(block_hash):
                            signal_block_buffer_stream(block_hash)  # Send the sub-buffer stop signal

                        yield buffer_obj
                        for block_chunk in parser_iterator(
                                request_iterator_obj=request_iterator_obj,
                                signal_obj=signal_obj,
                                blocks=blocks
                        ):
                            yield block_chunk

            if buffer_obj.HasField('chunk'):
                yield buffer_obj
            elif not buffer_obj.HasField('head'):
                break
            if buffer_obj.HasField('separator') and buffer_obj.separator:
                break

    def parse_message(message_field, _request_iterator, _signal: Signal):
        all_buffer: bytes = b''
        in_block: typing.Optional[str] = None
        for b in parser_iterator(
                request_iterator_obj=_request_iterator,
                signal_obj=_signal,
        ):
            if b.HasField('block'):
                id: str = get_hash_from_block(block=b.block)
                if id == in_block:
                    in_block = None

                elif not in_block and block_exists(block_id=id):
                    # TODO performance  could be on Thread or asnyc. with the read_block task.
                    #    for b in parse_iterator: if b.HasField('block') and get_hash..(b) == id: break
                    in_block: str = id
                    all_buffer += b''.join([c for c in read_block(block_id=id) if type(c) is bytes])
                    continue

            if not in_block:
                all_buffer += b.chunk

        if len(all_buffer) == 0:
            raise EmptyBufferException()
        if message_field is str:
            return all_buffer.decode('utf-8')
        elif inspect.isclass(message_field) and issubclass(message_field, Message):
            message = message_field()
            message.ParseFromString(
                all_buffer
            )
            return message
        else:
            try:
                return message_field(all_buffer)
            except Exception as e:
                raise Exception(
                    'gRPCbb error -> Parse message error: some primitive type message not suported for contain '
                    'partition ' + str(
                        message_field) + str(e))

    def save_to_dir(_request_iterator, _signal) -> str:
        dirname = generate_random_dir()
        _i: int = 1
        _json: List[Union[
            int,
            typing.Tuple[str, List[int]]
        ]] = []
        try:
            while True:
                _json.append(_i)
                if save_chunks_to_file(
                        filename=dirname + '/' + str(_i),
                        buffer_iterator=parser_iterator(
                            request_iterator_obj=_request_iterator,
                            signal_obj=_signal
                        ),
                        signal=_signal,
                        _json=_json
                ):
                    break
                _i += 1

        except StopIteration:
            pass

        except Exception as e:
            remove_dir(dir=dirname)
            raise e

        if len(_json) < 2:
            filename: str = generate_random_file()
            try:
                shutil.move(dirname + '/1', filename)
                return filename

            except FileNotFoundError:
                remove_file(file=filename)
                raise Exception('gRPCbb error: on save_to_dir function, the only file had no name 1')

        else:
            with open(dirname + '/' + METADATA_FILE_NAME, 'w') as f:
                json.dump(_json, f)

            generate_wbp_file(dirname)

            return dirname  # separator break.

    def iterate_message(message_field_or_route, _signal: Signal, _request_iterator):
        if message_field_or_route and type(message_field_or_route) is not str:
            return parse_message(
                message_field=message_field_or_route,
                _request_iterator=_request_iterator,
                _signal=_signal,
            )
        else:
            return save_to_dir(
                _request_iterator=_request_iterator,
                _signal=_signal
            )

    for buffer in request_iterator:
        # The order of conditions is important.
        if buffer.HasField('head'):
            if buffer.head.index not in indices:
                raise Exception(
                    'Parse from buffer error: buffer head index is not correct ' + str(buffer.head.index) + str(
                        indices.keys()))
            try:
                if not partitions_message_mode[buffer.head.index]:
                    # return a dir with some indices, specify the index is needed.
                    yield indices[buffer.head.index]

                yield iterate_message(
                    message_field_or_route=indices[buffer.head.index] if partitions_message_mode[buffer.head.index]
                        else None,
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )

            except EmptyBufferException:
                if indices[1] == buffer_pb2.Empty:
                    yield buffer_pb2.Empty()
                else:
                    continue

        elif 1 in indices:  # Does not've more than one index and more than one partition too.
            try:
                yield iterate_message(
                    message_field_or_route=indices[1] if partitions_message_mode[1] else None,
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )
            except EmptyBufferException:
                if indices[1] == buffer_pb2.Empty:
                    yield buffer_pb2.Empty()
                else:
                    continue

        elif 0 in indices:  # always true
            try:
                yield iterate_message(
                    message_field_or_route=indices[0] if partitions_message_mode[0] else None,
                    _signal=signal,
                    _request_iterator=itertools.chain([buffer], request_iterator),
                )
            except EmptyBufferException:
                if indices[0] == buffer_pb2.Empty:
                    yield buffer_pb2.Empty()
                else:
                    continue

        else:
            raise Exception('Parse from buffer error: index are not correct ' + str(indices))


def serialize_to_buffer(
        message_iterator=None,  # Message, bytes or Dir
        signal=None,
        indices: Union[Message, dict] = None,
        mem_manager=None
) -> Generator[buffer_pb2.Buffer, None, None]:  # method: indice
    try:
        if not message_iterator:
            message_iterator = buffer_pb2.Empty()
        if not indices:
            indices = {}
        if not signal:
            signal = Signal(exist=False)
        if not mem_manager:
            mem_manager = Enviroment.mem_manager
        if type(indices) is not dict:
            if issubclass(indices, Message):
                indices = {1: indices}
            else:
                raise Exception

        indices.update({0: bytes})
        if not hasattr(message_iterator, '__iter__') or type(message_iterator) is tuple:
            message_iterator = itertools.chain([message_iterator])

        if 1 not in indices:
            message_type = next(message_iterator)
            indices.update({1: message_type[0]}) if type(message_type) is tuple and message_type[
                0] != bytes else indices.update(
                {1: type(message_type)})
            message_iterator = itertools.chain([message_type], message_iterator)

        indices = {e[1]: e[0] for e in indices.items()}
    except Exception as e:
        raise Exception(f'Serialzie to buffer error: Indices are not correct {str(indices)} - {str(e)}')

    def send_file(_head: buffer_pb2.Buffer.Head, filedir: Dir, _signal: Signal) -> Generator[buffer_pb2.Buffer, None, None]:
        yield buffer_pb2.Buffer(
            head=_head
        )
        for _b in read_from_registry(
                filename=filedir.dir,
                signal=_signal
        ):
            _signal.wait()
            try:
                yield _b
            finally:
                _signal.wait()
        yield buffer_pb2.Buffer(
            separator=True
        )

    def send_message(
            _signal: Signal,
            _message: Message | bytes,
            _head: buffer_pb2.Buffer.Head = None,
            _mem_manager=Enviroment.mem_manager,
    ) -> Generator[buffer_pb2.Buffer, None, None]:

        message_bytes = message_to_bytes(message=_message)
        if len(message_bytes) < CHUNK_SIZE and (
                not isinstance(_message, Message) or
                isinstance(_message, Message) and not contain_blocks(message=_message)
        ):
            _signal.wait()
            try:
                yield buffer_pb2.Buffer(
                    chunk=bytes(message_bytes),
                    head=_head,
                    separator=True
                ) if _head else buffer_pb2.Buffer(
                    chunk=bytes(message_bytes),
                    separator=True
                )
            finally:
                _signal.wait()

        else:
            try:
                if _head:
                    yield buffer_pb2.Buffer(
                        head=_head
                    )
            finally:
                _signal.wait()

            _signal.wait()
            file = generate_random_file()
            with open(file, 'wb') as f, _mem_manager(len=len(message_bytes)):
                f.write(message_bytes)
            try:
                yield from read_from_registry(
                    filename=file,
                    signal=_signal
                )
            finally:
                remove_file(file)

            try:
                yield buffer_pb2.Buffer(
                    separator=True
                )
            finally:
                _signal.wait()

    for message in message_iterator:
        if type(message) is Dir:
            yield from send_file(
                _head=buffer_pb2.Buffer.Head(
                    index=indices[message.type]
                ),
                filedir=message.dir,
                _signal=signal
            )
        else:
            yield from send_message(
                _signal=signal,
                _message=message,
                _head=buffer_pb2.Buffer.Head(
                    index=indices[type(message)]
                ),
                _mem_manager=mem_manager,
            )


def client_grpc(
        method,
        input=None,
        timeout=None,
        indices_parser: Union[Message, dict] = None,
        partitions_message_mode_parser: Union[bool, list, dict] = None,
        indices_serializer: Union[Message, dict] = None,
        mem_manager=None,
):  # indice: method
    if not indices_parser:
        indices_parser = buffer_pb2.Empty
        partitions_message_mode_parser = True
    if not partitions_message_mode_parser: partitions_message_mode_parser = False
    if not indices_serializer: indices_serializer = {}
    if not mem_manager: mem_manager = Enviroment.mem_manager
    signal = Signal()
    yield from parse_from_buffer(
        request_iterator=method(
            serialize_to_buffer(
                message_iterator=input if input else buffer_pb2.Empty(),
                signal=signal,
                indices=indices_serializer,
                mem_manager=mem_manager,
            ),
            timeout=timeout
        ),
        signal=signal,
        indices=indices_parser,
        partitions_message_mode=partitions_message_mode_parser,
    )


"""
    Serialize Object to plain bytes serialization.
"""


def serialize_to_plain(object: object) -> bytes:
    pass
