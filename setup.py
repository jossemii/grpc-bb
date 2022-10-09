from setuptools import setup, find_packages

setup(
    name='grpcbigbuffer',
    version='__dev__',

    url='https://github.com/jossemii/GRPCBigBuffer_Client-Python.git',
    author='Josemi Avellana',
    author_email='josemi.bnf@gmail.com',

    py_modules=['grpcbigbuffer', 'buffer_pb2'],
    install_requires=[
        'grpcio==1.34.1',
        'grpcio-tools==1.34.1',
        'protobuf==3.19.4',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
)