import os
import sys
import uuid

import pytest
import sqlalchemy as db
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects import registry
from sqlalchemy.orm import backref, declarative_base, relationship, sessionmaker

module_dir = os.path.abspath("sqlalchemy-sqlitecloud")
if module_dir not in sys.path:
    sys.path.insert(0, module_dir)

registry.register("sqlitecloud", "sqlalchemy_sqlitecloud.base", "SQLiteCloudDialect")

Base = declarative_base()


class Artist(Base):
    __tablename__ = "artists"

    ArtistId = Column("ArtistId", Integer, primary_key=True)
    Name = Column("Name", String)
    Albums = relationship("Album", backref=backref("artist"))


class Album(Base):
    __tablename__ = "albums"

    AlbumId = Column("AlbumId", Integer, primary_key=True)
    ArtistId = Column("ArtistId", Integer, ForeignKey("artists.ArtistId"))
    Title = Column("Title", String)


@pytest.fixture()
def sqlitecloud_connection_string():
    connection_string = os.getenv("SQLITE_CONNECTION_STRING")
    database = os.getenv("SQLITE_DB")
    apikey = os.getenv("SQLITE_API_KEY")

    engine = db.create_engine(f"{connection_string}/{database}?apikey={apikey}")
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    engine.dispose()


@pytest.fixture()
def sqlite_connection_string():
    engine = db.create_engine("sqlite:///src/tests/assets/chinook.sqlite")
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    engine.dispose()


@pytest.mark.parametrize(
    "session", ["sqlitecloud_connection_string", "sqlite_connection_string"]
)
def test_insert_and_select(session, request):
    session = request.getfixturevalue(session)

    name = "Mattew" + str(uuid.uuid4())
    query = db.insert(Artist).values(Name=name)
    result_insert = session.execute(query)

    title = "The Album" + str(uuid.uuid4())
    query = db.insert(Album).values(ArtistId=result_insert.lastrowid, Title=title)
    session.execute(query)

    query = (
        db.select(Artist, Album)
        .join(Album, Artist.ArtistId == Album.ArtistId)
        .where(Artist.ArtistId == result_insert.lastrowid)
    )

    result = session.execute(query).fetchone()

    assert result[0].ArtistId == result_insert.lastrowid
    assert result[0].Name == name
    assert result[1].Title == title
