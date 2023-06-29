import inspect
import itertools
import json
import os
import shutil
import sys
import typing
import warnings
from random import randint
from typing import Generator, Union, List

from google import protobuf
from google.protobuf.message import DecodeError, Message
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer as RepeatedCompositeContainer

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
    if inspect.isclass(type(message)) and issubclass(message, Message):
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
        indices: Union[Message, dict] = None,
        # indice: method      message_field = None,
        partitions_model: Union[list, dict] = None,
        partitions_message_mode: Union[bool, list, dict] = False,  # Write on disk by default.
        mem_manager=None,
        yield_remote_partition_dir: bool = False,
):
    try:
        if not indices:
            indices = buffer_pb2.Empty()
        if not partitions_model:
            partitions_model = [buffer_pb2.Buffer.Head.Partition()]
        if not signal:
            signal = Signal(exist=False)
        if not mem_manager:
            mem_manager = Enviroment.mem_manager
        if type(indices) is not dict:
            if issubclass(indices, Message):
                indices = {1: indices}
            else:
                raise Exception

        if type(partitions_model) is list: partitions_model = {1: partitions_model}  # Only've one index.
        if type(partitions_model) is not dict: raise Exception
        for i in indices.keys():
            if i in partitions_model:
                if type(partitions_model[i]) is buffer_pb2.Buffer.Head.Partition: partitions_model[i] = [
                    partitions_model[i]]
                if type(partitions_model[i]) is not list: raise Exception
            else:
                partitions_model.update({i: [buffer_pb2.Buffer.Head.Partition()]})

        if type(partitions_message_mode) is bool:
            partitions_message_mode = {i: [partitions_message_mode for m in l] for i, l in
                                       partitions_model.items()}  # The same mode for all index and partitions.
        if type(partitions_message_mode) is list: partitions_message_mode = {
            1: partitions_message_mode}  # Only've one index.
        for i, l in partitions_message_mode.items():  # If an index in the partitions message mode have a boolean,
            # it applies for all partitions of this index.
            if type(l) is bool:
                partitions_message_mode[i] = [l for m in partitions_model[i]]
            elif type(l) is not list:
                raise Exception
        partitions_message_mode.update(
            {i: [False] for i in indices if i not in partitions_message_mode})  # Check that it've all indices.

        if partitions_message_mode.keys() != indices.keys(): raise Exception  # Check that partition modes' index're
        # correct.
        for i in indices.keys():  # Check if partitions modes and partitions have the same lenght in all indices.
            if len(partitions_message_mode[i]) != len(partitions_model[i]):
                raise Exception
    except:
        raise Exception('Parse from buffer error: Partitions or Indices are not correct.' + str(partitions_model) + str(
            partitions_message_mode) + str(indices))

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

    def iterate_partition(message_field_or_route, _signal: Signal, _request_iterator):
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

    def iterate_partitions(_signal: Signal, _request_iterator, partitions: list = None):
        if not partitions: partitions = [None]
        for _i, partition in enumerate(partitions):
            try:
                yield iterate_partition(
                    message_field_or_route=partition,
                    _signal=_signal,
                    _request_iterator=_request_iterator,
                )
            except EmptyBufferException:
                continue

    def conversor(
            iterator,
            pf_object: object = None,
            local_partitions_model: list = None,
            remote_partitions_model: list = None,
            _mem_manager=Enviroment.mem_manager,
            _yield_remote_partition_dir: bool = False,
            _partitions_message_mode: list = None,
    ):
        if not local_partitions_model: local_partitions_model = []
        if not remote_partitions_model: remote_partitions_model = []
        if not _partitions_message_mode: _partitions_message_mode = []
        yield pf_object
        dirs: List[str] = []
        # 1. Save the remote partitions on cache.
        try:
            for d in iterator:
                # 2. yield remote partitions directory.
                if _yield_remote_partition_dir:
                    yield d
                dirs.append(d)
        except EmptyBufferException:
            pass
        if not pf_object or 0 < len(remote_partitions_model) != len(dirs): return None
        # 3. Parse to the local partitions from the remote partitions using mem_manager.
        # TODO: check the limit memory formula.
        with _mem_manager(len=3 * sum([os.path.getsize(_dir) for _dir in dirs[:-1]]) + 2 * os.path.getsize(dirs[-1])):
            if (len(remote_partitions_model) == 0 or len(remote_partitions_model) == 1) and len(dirs) == 1:
                main_object = pf_object()
                d: str = dirs[0]
                is_dir: bool = False
                if os.path.isdir(d):
                    d = dirs[0] + '/' + WITHOUT_BLOCK_POINTERS_FILE_NAME
                    is_dir = True
                main_object.ParseFromString(open(d, 'rb').read())
                remove_file(file=d) if not is_dir else remove_dir(dir=d)
            elif len(remote_partitions_model) != len(dirs):
                raise Exception("Error: remote partitions model are not correct with the buffer.")
            else:
                main_object = combine_partitions(
                    obj_cls=pf_object,
                    partitions_model=tuple(remote_partitions_model),
                    partitions=tuple(dirs)
                )
                for directory in dirs:
                    remove_file(directory) if os.path.isfile(directory) else remove_dir(directory)

            # 4. yield local partitions.
            if not local_partitions_model: local_partitions_model.append(buffer_pb2.Buffer.Head.Partition())
            for i, partition in enumerate(local_partitions_model):
                if i + 1 == len(local_partitions_model):
                    aux_object = main_object
                    del main_object
                else:
                    aux_object = pf_object()
                    aux_object.CopyFrom(main_object)
                aux_object = get_submessage(partition=partition, obj=aux_object)
                message_mode = _partitions_message_mode[i]
                if not message_mode:
                    filename = generate_random_file()
                    with open(filename, 'wb') as f:
                        f.write(
                            aux_object.SerializeToString() if hasattr(aux_object, 'SerializeToString')
                            else bytes(aux_object) if type(aux_object) is not str else bytes(aux_object, 'utf8')
                        )
                    del aux_object
                    if i + 1 == len(local_partitions_model):
                        last = filename
                    else:
                        yield filename
                else:
                    if i + 1 == len(local_partitions_model):
                        last = aux_object
                        del aux_object
                    else:
                        yield aux_object
        yield last  # Necesario para evitar realizar una última iteración del conversor para salir del mem_manager,
        # y en su uso no es necesario esa última iteración porque se conoce local_partitions.

    for buffer in request_iterator:
        # The order of conditions is important.
        if buffer.HasField('head'):
            if buffer.head.index not in indices: raise Exception(
                'Parse from buffer error: buffer head index is not correct ' + str(buffer.head.index) + str(
                    indices.keys()))
            if not ((len(buffer.head.partitions) == 0 and len(partitions_model[buffer.head.index]) == 1) or
                    (len(buffer.head.partitions) == len(partitions_model[buffer.head.index]) and
                     list(buffer.head.partitions) == partitions_model[buffer.head.index])):  # If not match
                yield from conversor(
                        iterator=iterate_partitions(
                            partitions=[None for i in buffer.head.partitions] if len(buffer.head.partitions) > 0 else [
                                None],
                            _signal=signal,
                            _request_iterator=itertools.chain([buffer], request_iterator),
                        ),
                        local_partitions_model=partitions_model[buffer.head.index],
                        remote_partitions_model=buffer.head.partitions,
                        _mem_manager=mem_manager,
                        _yield_remote_partition_dir=yield_remote_partition_dir,
                        pf_object=indices[buffer.head.index],
                        _partitions_message_mode=partitions_message_mode[buffer.head.index],
                )

            elif len(partitions_model[buffer.head.index]) > 1:
                yield indices[buffer.head.index]
                yield from iterate_partitions(
                        partitions=[
                            get_subclass(object_cls=indices[buffer.head.index], partition=partition)
                            if partitions_message_mode[buffer.head.index][part_i] else None for
                            part_i, partition in enumerate(partitions_model[buffer.head.index])
                        ],
                        # TODO performance
                        _signal=signal,
                        _request_iterator=itertools.chain([buffer], request_iterator),
                )

            else:
                try:
                    if not partitions_message_mode[buffer.head.index][0]:
                        # return a dir with some indices and only one partition, specify the index is needed.
                        yield indices[buffer.head.index]

                    yield iterate_partition(
                        message_field_or_route=indices[buffer.head.index] if partitions_message_mode[buffer.head.index][
                            0] else None,
                        _signal=signal,
                        _request_iterator=itertools.chain([buffer], request_iterator),
                    )

                except EmptyBufferException:
                    if indices[1] == buffer_pb2.Empty:
                        yield buffer_pb2.Empty()
                    else: continue

        elif 1 in indices:  # Does not've more than one index and more than one partition too.
            if len(partitions_model[1]) > 1:
                yield from conversor(
                        iterator=iterate_partitions(
                            _signal=signal,
                            _request_iterator=itertools.chain([buffer], request_iterator),
                        ),
                        remote_partitions_model=[buffer_pb2.Buffer.Head.Partition()],
                        local_partitions_model=partitions_model[1],
                        _mem_manager=mem_manager,
                        _yield_remote_partition_dir=yield_remote_partition_dir,
                        pf_object=indices[1],
                        _partitions_message_mode=partitions_message_mode[1],
                )
            else:
                try:
                    yield iterate_partition(
                        message_field_or_route=indices[1] if partitions_message_mode[1][0] else None,
                        _signal=signal,
                        _request_iterator=itertools.chain([buffer], request_iterator),
                    )
                except EmptyBufferException:
                    if indices[1] == buffer_pb2.Empty:
                        yield buffer_pb2.Empty()
                    else: continue

        else:
            raise Exception('Parse from buffer error: index are not correct ' + str(indices))


def serialize_to_buffer(
        message_iterator=None,  # Message or tuples (with head on the first item.)
        signal=None,
        indices: Union[Message, dict] = None,
        partitions_model: Union[list, dict] = None,
        mem_manager=None
) -> Generator[buffer_pb2.Buffer, None, None]:  # method: indice
    try:
        if not message_iterator: message_iterator = buffer_pb2.Empty()
        if not indices: indices = {}
        if not partitions_model: partitions_model = [buffer_pb2.Buffer.Head.Partition()]
        if not signal: signal = Signal(exist=False)
        if not mem_manager: mem_manager = Enviroment.mem_manager
        if inspect.isclass(indices) and issubclass(indices, Message): indices = {1: indices}
        if type(indices) is not dict: raise Exception

        if type(partitions_model) is list: partitions_model = {1: partitions_model}  # Only've one index.
        if type(partitions_model) is not dict: raise Exception
        for i in indices.keys():
            if i in partitions_model:
                if type(partitions_model[i]) is buffer_pb2.Buffer.Head.Partition: partitions_model[i] = [
                    partitions_model[i]]
                if type(partitions_model[i]) is not list: raise Exception
            else:
                partitions_model.update({i: [buffer_pb2.Buffer.Head.Partition()]})

        if not hasattr(message_iterator, '__iter__') or type(message_iterator) is tuple:
            message_iterator = itertools.chain([message_iterator])

        if 1 not in indices:
            message_type = next(message_iterator)
            indices.update({1: message_type[0]}) if type(message_type) is tuple else indices.update(
                {1: type(message_type)})
            message_iterator = itertools.chain([message_type], message_iterator)

        indices = {e[1]: e[0] for e in indices.items()}
    except:
        raise Exception('Serialzie to buffer error: Indices are not correct ' + str(indices) + str(partitions_model))

    def send_file(filedir: Dir, _signal: Signal) -> Generator[buffer_pb2.Buffer, None, None]:
        for _b in read_from_registry(
                filename=filedir.name,
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
            _message: Message,
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
                if _head: yield buffer_pb2.Buffer(
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
        if type(message) is tuple:  # If is partitioned
            yield buffer_pb2.Buffer(
                head=buffer_pb2.Buffer.Head(
                    index=indices[message[0]],
                    partitions=partitions_model[indices[message[0]]]
                )
            )

            for partition in message[1:]:
                if type(partition) is Dir:
                    yield from send_file(
                            filedir=partition,
                            _signal=signal
                    )

                else:
                    yield from send_message(
                            _signal=signal,
                            _message=partition,
                            _mem_manager=mem_manager,
                    )

        else:  # If message is a protobuf object.
            head = buffer_pb2.Buffer.Head(
                index=indices[type(message)],
                partitions=partitions_model[indices[type(message)]]
            )
            yield from send_message(
                    _signal=signal,
                    _message=message,
                    _head=head,
                    _mem_manager=mem_manager,
            )


def client_grpc(
        method,
        input=None,
        timeout=None,
        indices_parser: Union[Message, dict] = None,
        partitions_parser: Union[list, dict] = None,
        partitions_message_mode_parser: Union[bool, list, dict] = None,
        indices_serializer: Union[Message, dict] = None,
        partitions_serializer: Union[list, dict] = None,
        mem_manager=None,
        yield_remote_partition_dir_on_serializer: bool = False,
):  # indice: method
    if not indices_parser:
        indices_parser = buffer_pb2.Empty
        partitions_message_mode_parser = True
    if not partitions_message_mode_parser: partitions_message_mode_parser = False
    if not partitions_parser: partitions_parser = [buffer_pb2.Buffer.Head.Partition()]
    if not indices_serializer: indices_serializer = {}
    if not partitions_serializer: partitions_serializer = [buffer_pb2.Buffer.Head.Partition()]
    if not mem_manager: mem_manager = Enviroment.mem_manager
    signal = Signal()
    yield from parse_from_buffer(
            request_iterator=method(
                serialize_to_buffer(
                    message_iterator=input if input else buffer_pb2.Empty(),
                    signal=signal,
                    indices=indices_serializer,
                    partitions_model=partitions_serializer,
                    mem_manager=mem_manager,
                ),
                timeout=timeout
            ),
            signal=signal,
            indices=indices_parser,
            partitions_model=partitions_parser,
            partitions_message_mode=partitions_message_mode_parser,
            yield_remote_partition_dir=yield_remote_partition_dir_on_serializer,
    )


"""
    Get partitions and return the protobuff buffer.
"""


def partitions_to_buffer(
        message: Message,
        partitions_model: tuple,
        partitions: tuple
) -> str:
    total_len = 0
    for partition in partitions:
        if type(partition) is str:
            total_len += 2 * os.path.getsize(partition)
        elif type(partition) is object:
            total_len += 2 * sys.getsizeof(partition)
        elif type(partition) is bytes:
            total_len += len(partition)
        else:
            raise Exception('Partition to buffer error: partition type is wrong: ' + str(type(partition)))
    with Enviroment.mem_manager(len=total_len):
        return combine_partitions(
            obj_cls=message,
            partitions_model=partitions_model,
            partitions=partitions
        ).SerializeToString()


"""
    Serialize Object to plain bytes serialization.
"""


def serialize_to_plain(object: object) -> bytes:
    pass
