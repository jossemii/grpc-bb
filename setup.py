from setuptools import setup

from grpcbigbuffer import __version__

setup(
    name='grpcbigbuffer',
    version=__version__,

    url='https://github.com/jossemii/GRPCBigBuffer_Client.git',
    author='Josemi Avellana',
    author_email='josemi.bnf@gmail.com',

    py_modules=['grpcbigbuffer'],
    install_requires=[
        'protobuf',
        'grpcio',
    ],
)