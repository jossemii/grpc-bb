import codecs
import hashlib
import json
import os
import subprocess
import sys
import unittest
from typing import List, Generator, Union, Tuple

sys.path.append('../src/')
from grpcbigbuffer.client import partitions_to_buffer
from grpcbigbuffer import block_builder
from grpcbigbuffer.block_driver import generate_wbp_file
from grpcbigbuffer import celaut_pb2 as celaut, compile_pb2
from grpcbigbuffer import client as grpcbb

GET_ENV = lambda env, default: (type(default)(os.environ.get(env)) if type(default) != bool
                                else os.environ.get(env) in ['True', 'true', 'T',
                                                             't']) if env in os.environ.keys() else default

MIN_BUFFER_BLOCK_SIZE = GET_ENV(env='MIN_BUFFER_BLOCK_SIZE', default=10 ** 7)

CACHE = '__cache__/'
COMPILER_SUPPORTED_ARCHITECTURES = [  # The first element of each list is the Docker buildx tag.
    ['linux/arm64', 'arm64', 'arm_64', 'aarch64'] if GET_ENV(env='ARM_COMPILER_SUPPORT', default=True) else [],
    ['linux/amd64', 'x86_64', 'amd64'] if GET_ENV(env='X86_COMPILER_SUPPORT', default=False) else []
]
DOCKER_COMMAND = subprocess.check_output(["which", "docker"]).strip().decode("utf-8")

# -- HASH IDs --
SHAKE_256_ID = bytes.fromhex("46b9dd2b0ba88d13233b3feb743eeb243fcd52ea62b81b82b50c27646ed5762f")
SHA3_256_ID = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")

# -- HASH FUNCTIONS --
SHAKE_256 = lambda value: "" if value is None else hashlib.shake_256(value).digest(32)
SHA3_256 = lambda value: "" if value is None else hashlib.sha3_256(value).digest()

HASH_FUNCTIONS = {
    SHA3_256_ID: SHA3_256,
    SHAKE_256_ID: SHAKE_256
}


def calculate_hashes(value: bytes) -> List[celaut.Any.Metadata.HashTag.Hash]:
    return [
        celaut.Any.Metadata.HashTag.Hash(
            type=SHA3_256_ID,
            value=SHA3_256(value)
        ),
        celaut.Any.Metadata.HashTag.Hash(
            type=SHAKE_256_ID,
            value=SHAKE_256(value)
        )
    ]


# Return the service's sha3-256 hash on hexadecimal format.
def get_service_hex_main_hash(
        service_buffer: Union[bytes, str, celaut.Service, tuple, None] = None,
        partitions_model: tuple = None,
        metadata: celaut.Any.Metadata = None,
        other_hashes: list = None
) -> str:
    # Find if it has the hash.
    if other_hashes is None:
        other_hashes = []
    if metadata is None:
        metadata = celaut.Any.Metadata()

    for hash in list(metadata.hashtag.hash) + other_hashes:
        if hash.type == SHA3_256_ID:
            return hash.value.hex()

    # If not but the spec. is complete, calculate the hash pruning it before.
    # If not and is incomplete, it's going to be impossible calculate any hash.

    if not service_buffer:
        print(' sha3-256 hash function is not implemented on this method.')
        raise Exception(' sha3-256 hash function is not implemented on this method.')

    if type(service_buffer) is not tuple:
        try:
            return SHA3_256(
                value=service_buffer if type(service_buffer) is bytes
                else open(service_buffer, 'rb').read() if type(service_buffer) is str
                else celaut.Service(service_buffer).SerializeToString()
            ).hex()
        except Exception as e:
            print('Exception getting a service hash: ' + str(e))
            pass

    elif partitions_model:
        return SHA3_256(
            value=partitions_to_buffer(
                message_type=celaut.Service,
                partitions_model=partitions_model,
                partitions=service_buffer
            )
        ).hex()


def calculate_hashes_by_stream(value: Generator[bytes, None, None]) -> List[celaut.Any.Metadata.HashTag.Hash]:
    sha3_256 = hashlib.sha3_256()
    shake_256 = hashlib.shake_256()
    for chunk in value:
        sha3_256.update(chunk)
        shake_256.update(chunk)

    return [
        celaut.Any.Metadata.HashTag.Hash(
            type=SHA3_256_ID,
            value=sha3_256.digest()
        ),
        celaut.Any.Metadata.HashTag.Hash(
            type=SHAKE_256_ID,
            value=shake_256.digest(32)
        )
    ]

def get_dir_size(path='.'):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_symlink():
                pass
            elif entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total


def get_service_list_of_hashes(service_buffer: bytes, metadata: celaut.Any.Metadata, complete: bool = True) -> list:
    if complete:
        return calculate_hashes(
            value=service_buffer
        )
    else:
        raise Exception("Can't get the hashes if the service is not complete.")


class Hyper:
    def __init__(self, path, aux_id, build_it):
        super().__init__()
        self.blocks: List[bytes] = []
        self.service = compile_pb2.Service()
        self.metadata = celaut.Any.Metadata()
        self.path = path
        self.aux_id = aux_id
        self.build_it = build_it

        if self.build_it:
            self.json = json.load(open(self.path + "service.json", "r"))
            arch = None
            for a in COMPILER_SUPPORTED_ARCHITECTURES:
                if self.json.get('architecture') in a: arch = a[0]

            if not arch: raise Exception("Can't compile this service, not supported architecture.")

            # Directories are created on cache.
            os.system("mkdir " + CACHE + self.aux_id + "/building")
            os.system("mkdir " + CACHE + self.aux_id + "/filesystem")

            # Build container and get compressed layers.
            if not os.path.isfile(self.path + 'Dockerfile'): raise Exception("Error: Dockerfile no encontrado.")
            os.system(
                DOCKER_COMMAND + ' buildx build --platform ' + arch + ' --no-cache -t builder' + self.aux_id + ' ' + self.path)
            os.system(
                DOCKER_COMMAND + " save builder" + self.aux_id + " > " + CACHE + self.aux_id + "/building/container.tar")
            os.system(
                "tar -xvf " + CACHE + self.aux_id + "/building/container.tar -C " + CACHE + self.aux_id + "/building/")

            self.buffer_len = int(
                subprocess.check_output([DOCKER_COMMAND + " image inspect builder" + aux_id + " --format='{{.Size}}'"],
                                        shell=True))

    def parseContainer(self):
        def parseFilesys() -> celaut.Any.Metadata.HashTag:
            if self.build_it:
                # Save his filesystem on cache.
                for layer in os.listdir(CACHE + self.aux_id + "/building/"):
                    if os.path.isdir(CACHE + self.aux_id + "/building/" + layer):
                        print('Unzipping layer ' + layer)
                        os.system(
                            "tar -xvf " + CACHE + self.aux_id + "/building/" + layer + "/layer.tar -C "
                            + CACHE + self.aux_id + "/filesystem/"
                        )

            # Add filesystem data to filesystem buffer object.
            def recursive_parsing(directory: str) -> celaut.Service.Container.Filesystem:
                host_dir = CACHE + self.aux_id + "/filesystem" if self.build_it else self.path
                filesystem = celaut.Service.Container.Filesystem()
                for b_name in os.listdir(host_dir + directory):
                    if b_name == '.wh..wh..opq':
                        # https://github.com/opencontainers/image-spec/blob/master/layer.md#opaque-whiteout
                        continue
                    branch = celaut.Service.Container.Filesystem.ItemBranch()
                    branch.name = os.path.basename(b_name)

                    if len(branch.name) == 0:
                        continue

                    # It's a link.
                    if os.path.islink(host_dir + directory + b_name):
                        branch.link.dst = directory + b_name
                        branch.link.src = os.path.realpath(host_dir + directory + b_name)[
                                          len(host_dir):] if host_dir in os.path.realpath(
                            host_dir + directory + b_name) else os.path.realpath(host_dir + directory + b_name)

                        if branch.link.ByteSize() == 0 or len(branch.link.dst) == 0 or len(branch.link.src) == 0:
                            continue

                    # It's a file.
                    elif os.path.isfile(host_dir + directory + b_name):
                        if os.path.getsize(host_dir + directory + b_name) < MIN_BUFFER_BLOCK_SIZE:
                            with open(host_dir + directory + b_name, 'rb') as file:
                                branch.file = file.read()
                            
                            if len(branch.file) == 0:
                                continue

                        else:
                            block_hash, block = block_builder.create_block(
                                file_path=host_dir + directory + b_name,
                                copy=True
                            )
                            branch.file = block.SerializeToString()
                            if len(branch.file) == 0:
                                continue

                            if block_hash not in self.blocks:
                                self.blocks.append(block_hash)

                    # It's a folder.
                    elif os.path.isdir(host_dir + directory + b_name):
                        branch_filesystem = recursive_parsing(directory=directory + b_name + '/')
                        if branch_filesystem.ByteSize() > 0:
                            branch.filesystem.CopyFrom(
                                branch_filesystem    
                            )
                        else:
                            continue

                    filesystem.branch.append(branch)

                return filesystem

            self.service.container.filesystem.CopyFrom(recursive_parsing(directory="/"))

            return celaut.Any.Metadata.HashTag(
                hash=calculate_hashes(
                    value=self.service.container.filesystem.SerializeToString()
                ) if not self.blocks else
                calculate_hashes_by_stream(
                    value=grpcbb.read_multiblock_directory(
                        directory=block_builder.build_multiblock(
                            pf_object_with_block_pointers=self.service.container.filesystem,
                            blocks=self.blocks
                        )[1],
                        delete_directory=True,
                        ignore_blocks=True
                    )
                )
            )

        parseFilesys()

    def save(self) -> Tuple[str, Union[str, compile_pb2.ServiceWithMeta]]:

        # Generate the hashes.
        service_with_meta_obj = compile_pb2.ServiceWithMeta(
                metadata=self.metadata,
                service=self.service
            )

        bytes_id, directory = block_builder.build_multiblock(
            pf_object_with_block_pointers=service_with_meta_obj,
            blocks=self.blocks
        )
        service_id: str = codecs.encode(bytes_id, 'hex').decode('utf-8')

        # Generate the service with metadata.
        content_id, service_with_meta = block_builder.build_multiblock(
            pf_object_with_block_pointers=service_with_meta_obj,
            blocks=self.blocks
        )

        print('\nBloques -> ', self.blocks)
        print('\n Generate wbp file.')
        os.system('rm ' + service_with_meta + '/wbp.bin')
        try:
            generate_wbp_file(service_with_meta)
        except Exception as e:
            print(e, '\nNo se genero correctamente el wbp file.')
        print('\n File generated OK.')

        from hashlib import sha3_256
        from grpcbigbuffer import client as grpc_c
        validate_content = sha3_256()
        for i in grpc_c.read_multiblock_directory(service_with_meta):
            validate_content.update(i)

        return service_id, service_with_meta


class TestRealFilesystemCompiledBuffer(unittest.TestCase):
    def test(self):
        os.system('rm -rf __cache__/*')
        os.system('rm -rf __block__/*')
        
        build_it = False

        path = '__filesystem__/for_build/' if build_it else '__filesystem__/filesystem'
        os.mkdir(CACHE+'fs')

        spec_file = Hyper(path=path, aux_id='fs', build_it=build_it)
        spec_file.parseContainer()
        identifier, service_with_meta = spec_file.save()


if __name__ == "__main__":
    os.system('rm -rf __cache__/*')
    os.system('rm -rf __block__/*')
    TestRealFilesystemCompiledBuffer().test()