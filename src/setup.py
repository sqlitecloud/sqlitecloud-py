import io
import os

from setuptools import setup, find_packages


def read_file(filename):
    base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, filename)
    try:
        with io.open(full_path, encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        full_path = os.path.join(base_path, f'../{filename}')
        with io.open(full_path, encoding='utf-8') as file:
            return file.read()


setup(
    name='SqliteCloud',
    version='0.0.75',
    author='sqlitecloud.io',
    description='A Python package for working with SQLite databases in the cloud.',
    long_description=read_file('README-PYPI.md'),
    long_description_content_type='text/markdown',
    url="https://github.com/sqlitecloud/python",
    packages=find_packages(),
    install_requires=[
        'lz4 == 3.1.10',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    license='MIT',
)
