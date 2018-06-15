#!/usr/bin/python3

from sqlalchemy import create_engine
import psycopg2
import psycopg2.extensions

import pandas as pd
import numpy as np

import io
import time
import gzip

import unittest
import pytest

DATA_FILE = 'data/licenses.csv.gz'

DROP_TABLE = "DROP TABLE IF EXISTS licenses"

CREATE_TABLE = """
    CREATE TABLE licenses (
        a VARCHAR(16),
        b CHAR(3),
        c CHAR(6),
        d INTEGER,
        e INTEGER,
        f INTEGER
    )
"""

COPY_FROM = """
    COPY licenses (a, b, c, d, e, f) FROM STDIN
    WITH (FORMAT CSV, DELIMITER ';', HEADER)
"""

COPY_TO = "COPY licenses TO STDOUT WITH (FORMAT CSV, HEADER)"

SELECT_FROM = 'SELECT * FROM licenses'

VACUUM = "VACUUM FULL ANALYZE"

DB_UNIX_SOCKET_URL = 'postgresql://test:test@/test'

DB_TCP_URL = 'postgresql://test:test@db/test'

def my_cursor_factory(*args, **kwargs):
    cursor = psycopg2.extensions.cursor(*args, **kwargs)
    cursor.itersize = 10240
    return cursor

class TestImportDataSpeed(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setupBenchmark(self, benchmark):
        self.benchmark = benchmark

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(DB_UNIX_SOCKET_URL, connect_args={'cursor_factory': my_cursor_factory})
        connection = cls.engine.connect().connection
        cursor = connection.cursor()

        cursor.execute(DROP_TABLE)
        cursor.execute(CREATE_TABLE)

        with gzip.open(DATA_FILE, 'rb') as f:
            cursor.copy_expert(COPY_FROM, file=f, size=1048576)

        connection.commit()

        connection.set_session(autocommit=True)
        cursor.execute(VACUUM)

        cursor.close()


    def test_csv(self):

        def result():
            return pd.read_csv(DATA_FILE, delimiter=';')

        df = self.benchmark(result)
        assert df.shape == (3742616, 6)

    def test_db(self):

        def result():
            return pd.read_sql_query(SELECT_FROM, self.engine)

        df = self.benchmark(result)
        assert df.shape == (3742616, 6)

    def test_db_chunked(self):

        def result():
            gen = (x for x in pd.read_sql(SELECT_FROM, self.engine, chunksize=10240))
            return pd.concat(gen, ignore_index=True)

        df = self.benchmark(result)
        assert df.shape == (3742616, 6)

    def test_pg_copy(self):
        connection = self.engine.connect().connection
        cursor = connection.cursor()

        def result(cursor):
            f = io.StringIO()
            cursor.copy_expert(COPY_TO, file=f, size=1048576)
            f.seek(0)
            return pd.read_csv(f)

        df = self.benchmark(result, cursor)
        assert df.shape == (3742616, 6)
