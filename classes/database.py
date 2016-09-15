import sqlite3

import utils
import constants

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS statistics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object_type TEXT NOT NULL,
        object_id TEXT NOT NULL,
        cost REAL NOT NULL
    )
"""

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

    def __params_generator(self, statistics):
        for key, value in statistics.iteritems():
            obj_type, obj_id = key
            yield (obj_type, obj_id, value)

    def get_statistics(self):
        select_statistics = "SELECT object_type, object_id, cost FROM statistics"
        cursor = self.conn.cursor()
        cursor.execute(select_statistics)
        cursor.close()
        data = cursor.fetchmany()
        return data

    @utils.timeit
    def save_statistics(self, statistics):
        # get statistics from db as dic
        # merge dicts
        # save statistics

        insert_sql = "INSERT INTO statistics(object_type, object_id, cost) VALUES (?, ?, ?)"
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.executemany(insert_sql, self.__params_generator(statistics))
        self.conn.commit()
        cursor.close()
