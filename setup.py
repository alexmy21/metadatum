# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# This call to setup() does all the work
setup(
    name="metadatum",
    version="0.2.7",
    description="Demo library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://metadatum.readthedocs.io/",
    author="Alex Mylnikov",
    author_email="alexmy@lisa-park.com",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent"
    ],
    packages=["metadatum"],
    include_package_data=True,
    install_requires=[
        "numpy",
        "anyio",
        "Cerberus",
        "click",
        "h11",
        "idna==3.4",
        "numpy",
        "polars",
        "PyYAML",
        "redis",
        "sniffio",
        "starlette",
        "uvicorn",
        "cymple",
        "dynaconf",
        "textract"]
)