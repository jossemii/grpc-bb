from setuptools import setup, find_packages

from grpcbigbuffer import __version__

setup(
    name='grpcbigbuffer',
    version=__version__,

    url='https://github.com/jossemii/GRPCBigBuffer_Client.git',
    author='Josemi Avellana',
    author_email='josemi.bnf@gmail.com',

    packages=find_packages(),
    install_requires=[
        'grpcio',
        'protobuf',
    ],
)