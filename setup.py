from setuptools import setup, find_packages

setup(
    name='SqliteCloud',
    version='0.1.25',
    author='Sam and Pietro',
    description='A Python package for working with SQLite databases in the cloud.',
    packages=find_packages(),
    install_requires=[
        # Add your dependencies here
    ],
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
