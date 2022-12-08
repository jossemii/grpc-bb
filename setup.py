from setuptools import setup, find_packages

setup(
    name='grpcbigbuffer',
    version='0.0.4',

    url='https://github.com/jossemii/GRPCBigBuffer.git',
    author='Josemi Avellana',
    author_email='josemi.bnf@gmail.com',

    py_modules=[
        'grpcbigbuffer'
    ],
    install_requires=[
        'grpcio==1.48.1',
        'protobuf==3.19.4',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
)