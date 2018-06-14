#!/usr/bin/python3

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
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

COPY_CSV = """
    COPY licenses (a, b, c, d, e, f) FROM STDIN
    WITH (FORMAT CSV, DELIMITER ';', HEADER)
"""

class TestImportDataSpeed(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('importing data')
        cls.engine = create_engine('postgresql://test:test@db/test')
        connection = cls.engine.connect().connection
        cursor = connection.cursor()

        cursor.execute(DROP_TABLE)
        cursor.execute(CREATE_TABLE)

        with gzip.open(DATA_FILE, 'rb') as f:
            cursor.copy_expert(COPY_CSV, file=f, size=1048576)

        connection.commit()

        print('imported data')

    @classmethod
    def tearDownClass(cls):
        pass

    def test_csv(self, benchmark):

        @benchmark
        def result():
            print('test_csv')
            df = pd.read_csv(DATA_FILE)
            df *= 3

        assert result is None

    def test_db(self, benchmark):

        @benchmark
        def result():
            print('test_db')
            df = pd.read_sql_query('SELECT * FROM licenses', self.engine)
            df *= 3

        assert result is None
