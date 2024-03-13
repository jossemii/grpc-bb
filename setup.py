from setuptools import setup, find_packages

setup(
    name='pee-rpc',
    version='0.1.0',

    url='https://github.com/pee-rpc/pee-rpc.git',

    py_modules=[
        'pee-rpc'
    ],
    install_requires=[
        'grpcio==1.56.0',
        'protobuf==4.23.3',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.11",
)
