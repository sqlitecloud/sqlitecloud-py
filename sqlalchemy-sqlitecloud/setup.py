from pathlib import Path

from setuptools import find_packages, setup

long_description = (Path(__file__).parent / "README.md").read_text()

setup(
    name="sqlalchemy-sqlitecloud",
    version="0.1.2",
    author="sqlitecloud.io",
    description="SQLAlchemy Dialect for SQLite Cloud.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sqlitecloud/sqlitecloud-py",
    packages=find_packages(),
    install_requires=[
        "sqlitecloud >= 0.0.83",
    ],
    keywords="SQLAlchemy SQLite Cloud",
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
    entry_points={
        "sqlalchemy.dialects": [
            "sqlitecloud = sqlalchemy_sqlitecloud.base:SQLiteCloudDialect",
        ]
    },
)
