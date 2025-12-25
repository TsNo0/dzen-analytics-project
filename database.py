import sqlite3

class AnaliticsDatabase:
    def __init__(self, path):    
        self.__conn = sqlite3.Connection(path)
        self.__conn.execute("""CREATE TABLE IF NOT EXISTS author (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        name TEXT)""")
        self.__conn.execute("""CREATE TABLE IF NOT EXISTS category (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        name TEXT)""")
        self.__conn.execute("""CREATE TABLE IF NOT EXISTS content_type (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            name TEXT NOT NULL)""")
        self.__conn.execute("""CREATE TABLE IF NOT EXISTS content (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        type INTEGER REFERENCES content_type(id),
                        author_id INTEGER REFERENCES author(id),
                        name TEXT,
                        url TEXT,
                        watches INTEGER,
                        published_date INTEGER,
                        duration_seconds INTEGER,
                        created_note_datetime INTEGER)""")
        self.__conn.execute("""CREATE TABLE IF NOT EXISTS content_category (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        category_id INTEGER NOT NULL REFERENCES category(id),
                        content_id INTEGER NOT NULL REFERENCES content(id))""")

        self.__conn.execute("""INSERT OR IGNORE INTO content_type
                             VALUES (1, 'VIDEO'), (2, 'ARTICLE')""")
        self.__conn.commit()
    
    


db = AnaliticsDatabase("test.sqlite")
