import sqlite3

import utils
import constants

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS statistics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object_type TEXT NOT NULL,
        object_id TEXT NOT NULL,
        cost REAL NOT NULL
    );
"""

CREATE_INDEX_SQL = "CREATE UNIQUE INDEX IF NOT EXISTS obj_type_id_idx ON statistics (object_type, object_id);"


class Database(object):
    def __init__(self):
        self.conn = sqlite3.connect(constants.DB_FILE_PATH)
        self.__create_table()

    def __del__(self):
        self.conn.close()
        del self.conn

    def __create_table(self):
        self.conn.execute(CREATE_TABLE_SQL)
        self.conn.execute(CREATE_INDEX_SQL)
        self.conn.commit()

    def __params_generator(self, statistics):
        for key, value in statistics.iteritems():
            obj_type, obj_id = key
            yield (obj_type, obj_id, value)

    def clear_statistics(self):
        select_statistics = "DELETE FROM statistics"
        cursor = self.conn.cursor()
        cursor.execute(select_statistics)
        self.conn.commit()
        cursor.close()

    @utils.timeit
    def save_statistics(self, statistics):
        self.clear_statistics()
        insert_sql = "INSERT INTO statistics(object_type, object_id, cost) VALUES(?, ?, ?)"
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.executemany(insert_sql, self.__params_generator(statistics))
        self.conn.commit()
        cursor.close()
