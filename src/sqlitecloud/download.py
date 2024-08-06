import logging
from io import BufferedWriter

from sqlitecloud.datatypes import SQLiteCloudConnect
from sqlitecloud.driver import Driver


def xCallback(
    fd: BufferedWriter, data: bytes, blen: int, ntot: int, nprogress: int
) -> None:
    """
    Callback function used for downloading data.
    Data is passed to the callback to be written to the file and to
    monitor the progress.

    Args:
        fd (BufferedWriter): The file descriptor to write the downloaded data to.
        data (bytes): The data to be written.
        blen (int): The length of the data.
        ntot (int): The total length of the data being downloaded.
        nprogress (int): The number of bytes already downloaded.
    """
    fd.write(data)

    if blen == 0:
        logging.log(logging.DEBUG, "DOWNLOAD COMPLETE")
    else:
        logging.log(logging.DEBUG, f"{(nprogress + blen) / ntot * 100:.2f}%")


def download_db(connection: SQLiteCloudConnect, dbname: str, filename: str) -> None:
    """
    Download a database from the server.

    Raises:
        SQLiteCloudException: If an error occurs while downloading the database.
    """
    driver = Driver()

    with open(filename, "wb") as fd:
        driver.download_database(connection, dbname, fd, xCallback, False)
