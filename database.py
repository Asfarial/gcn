import psycopg2
from psycopg2 import sql
from psycopg2._psycopg import connection as psycopg2_connection
from psycopg2._psycopg import cursor as psycopg2_cursor
from core import Verbose
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DATABASE
from typing import Any, Collection


class DatabaseBase(Verbose):
    _user = DB_USER
    _password = DB_PASSWORD
    _host = DB_HOST
    _port = DB_PORT
    _database = DATABASE
    _schema =   """
                id SERIAL PRIMARY KEY,
                ean VARCHAR(32) UNIQUE NOT NULL,
                title TEXT,
                description TEXT,
                price REAL,
                old_price REAL,
                status VARCHAR(256),
                brand VARCHAR(256),
                color VARCHAR(256),
                url TEXT,
                discount VARCHAR(4)
                """

    def __init__(self,
                 user: str = _user,
                 password: str = _password,
                 host: str = _host,
                 port: str = _port,
                 database: str = _database,
                 *args, **kwargs):
        Verbose.__init__(self, *args, **kwargs)
        self._message("Initializing Database")
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._connection = self._connect()
        self._cursor = self._get_cursor()
        self._message("Connected to Database")

    def __del__(self):
        self._disconnect()

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def database(self):
        return self._database

    @property
    def connection(self):
        return self._connection

    @property
    def cursor(self):
        return self._cursor

    def _connect(self) -> psycopg2_connection:
        return psycopg2.connect(user=self.user,
                                password=self.password,
                                host=self.host,
                                port=self.port,
                                database=self.database)

    def _get_cursor(self) -> psycopg2_cursor:
        return self.connection.cursor()

    def _disconnect(self) -> None:
        self.cursor.close()
        self.connection.close()


class Database(DatabaseBase):
    @staticmethod
    def conv_float(val: str) -> float:
        """
        Converter
        field normalizer
        """
        if not val:
            return 0
        return float(val)

    def _get_csv_eans_records(self, csv_records: list) -> list[str]:
        return [ean for record in csv_records if (ean:=record.get('ean', None))]

    def _unpack_db_eans_records(self, db_eans_records: list[tuple[int]]) -> list[str]:
        return [str(db_record[0]) for db_record in db_eans_records]

    def _get_db_eans_records(self, table:str) -> list[str]:
        self._message("Comparing Records")
        query = sql.SQL("""
                                SELECT ean FROM {}
                                """).format(sql.Identifier(table))
        self.execute_query(query)
        db_eans_records = self.fetch()
        return self._unpack_db_eans_records(db_eans_records)

    @staticmethod
    def _compare_db_csv(
                        db_eans_records: list[str],
                        csv_eans_records: list[str]) -> list[str]:
        return [csv_record for csv_record in csv_eans_records if csv_record not in db_eans_records]

    def _columns_from_schema(self):
        return [col for column in DatabaseBase._schema.strip().split(',') if (col:=column.strip().split(" ")[0].strip())!='id']

    def add_records(self, records_to_add: list, csv_records: list, table:str):
        self._message(f"Adding missing records - {len(records_to_add)}")
        for record in csv_records:
            record['price'] = self.conv_float(record['price'])
            record['old_price'] = self.conv_float(record['old_price'])
        records_to_add = [tuple(record.values()) for record in csv_records if record.get('ean', '') in records_to_add]
        columns = self._columns_from_schema()
        query = sql.SQL("""
                INSERT INTO {} ({})
                VALUES {}
                """).format(sql.Identifier(table),
                            sql.SQL(',').join(map(sql.Identifier, columns)),
                            sql.SQL(',').join(map(sql.Literal, records_to_add)))
        self.execute_query(query)
        self.commit()
        self._message('Successfully added records')

    def compare_records(self, csv_records: list, table: str) -> list[str]:
        csv_eans_records = self._get_csv_eans_records(csv_records)
        db_eans_records = self._get_db_eans_records(table)
        return self._compare_db_csv(db_eans_records, csv_eans_records)

    def create_table(self, table:str, schema: str = DatabaseBase._schema) -> None:
        self._message(f"Creating table: {table}")
        query = sql.SQL("""
                CREATE TABLE {} (
                {schema}
                );
                """.format('{}', schema=schema)).format(sql.Identifier(table))
        self._message(query.as_string(self.connection))
        self.execute_query(query)
        self.commit()
        self._message(f"Table created: {table}")

    def drop_table(self, table:str) -> None:
        print(f"Dropping table: {table}")
        query = sql.SQL("""
                DROP TABLE {};
                """).format(sql.Identifier(table))
        if not input("Are you sure? y/yes").strip().lower() in ['y','yes']:
            print("Operation Aborted")
            return
        self.execute_query(query)
        self.commit()
        self._message(f"Table dropped: {table}")

    def table_exists(self, table:str) -> bool:
        self._message(f"Checking if Table {table} exists")
        self.execute_query("select * from information_schema.tables where table_name=%s", (table,))
        if result := bool(self.fetch()):
            self._message(f"Table exists: {table}")
        else:
            self._message(f"Table does not exists: {table}")
        return result

    def execute_query(self, query: str, q_args: Collection = None) -> None:
        self.cursor.execute(query, q_args)

    def fetch(self, how_many: int = 0) -> list[tuple[Any]]:
        match how_many:
            case 0:
                return self.cursor.fetchall()
            case 1:
                return [self.cursor.fetchone()]
            case _:
                return self.cursor.fetchmany(how_many)

    def commit(self) -> None:
        self.connection.commit()
