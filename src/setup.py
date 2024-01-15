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
    version='0.0.30',
    author='Sam Reghenzi & Matteo Fredi',
    description='A Python package for working with SQLite databases in the cloud.',
    long_description=read_file('README-PYPI.md'),
    long_description_content_type='text/markdown',
    url="https://github.com/sqlitecloud/python",
    packages=find_packages(),
    install_requires=[
        'mypy == 1.6.1',
        'mypy-extensions == 1.0.0',
        'typing-extensions == 4.8.0',
        'black == 23.7.0',
        'python-dotenv == 1.0.0',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    license='MIT',
)
