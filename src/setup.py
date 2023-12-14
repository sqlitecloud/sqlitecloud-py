from setuptools import setup, find_packages

setup(
    name='SqliteCloud',
    version='0.1.0',
    author='Sam Reghenzi & Matteo Fredi',
    description='A Python package for working with SQLite databases in the cloud.',

    long_description_content_type='text/markdown',

    url="https://github.com/codermine/sqlitecloud-python-sdk",
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
