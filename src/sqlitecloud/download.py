from io import BufferedWriter
import logging

from sqlitecloud.driver import Driver
from sqlitecloud.types import SQCloudConnect


def xCallback(
    fd: BufferedWriter, data: bytes, blen: int, ntot: int, nprogress: int
) -> None:
    fd.write(data)

    if blen == 0:
        logging.log(logging.DEBUG, "DOWNLOAD COMPLETE")
    else:
        logging.log(logging.DEBUG, f"{(nprogress + blen) / ntot * 100:.2f}%")


def download_db(connection: SQCloudConnect, dbname: str, filename: str) -> None:
    driver = Driver()

    with open(filename, "wb") as fd:
        driver.download_database(connection, dbname, fd, xCallback, False)
