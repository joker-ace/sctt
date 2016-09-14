import sqlite3

import helpers
import constants

# noinspection SqlNoDataSourceInspection
CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS statistics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object_type TEXT NOT NULL,
        object_id TEXT NOT NULL,
        cost REAL NOT NULL
    )
"""


def insert_params_list(statistics):
    for key, value in statistics.iteritems():
        obj_type, obj_id = key
        yield (obj_type, obj_id, value)


class Database(object):
    def __init__(self):
        self.conn = sqlite3.connect(constants.DB_FILE_PATH)
        self.__create_table()

    def __del__(self):
        self.conn.close()
        del self.conn

    def __create_table(self):
        self.conn.execute(CREATE_TABLE_SQL)
        self.conn.commit()

    @helpers.timeit
    def save_statistics(self, statistics):
        insert_sql = "INSERT INTO statistics(object_type, object_id, cost) VALUES (?, ?, ?)"
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.executemany(insert_sql, insert_params_list(statistics))
        self.conn.commit()
        cursor.close()
