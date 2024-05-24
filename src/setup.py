from pathlib import Path

from setuptools import find_packages, setup

# read the contents of your README file

long_description = (Path(__file__).parent / "README.md").read_text()

setup(
    name="SqliteCloud",
    version="0.0.76",
    author="sqlitecloud.io",
    description="A Python package for working with SQLite databases in the cloud.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sqlitecloud/python",
    packages=find_packages(),
    install_requires=[
        "lz4 >= 3.1.10",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    license="MIT",
)
