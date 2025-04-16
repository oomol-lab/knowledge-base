from sqlite3 import Connection, Cursor

FRAMEWORK_DB = "framework.db"
ConnSession = tuple[Cursor, Connection]