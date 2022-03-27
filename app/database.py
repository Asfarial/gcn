"""
Module
To operate Database
"""
from typing import Any, Collection
import psycopg2
from psycopg2 import sql
from app.core import Verbose
from app.config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DATABASE


class DatabaseBase(Verbose):
    """
    Database Low-Level Interface
    """
    _user = DB_USER
    _password = DB_PASSWORD
    _host = DB_HOST
    _port = DB_PORT
    _database = DATABASE
    _schema = """
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
    schema = _schema

    def __init__(
            self,
            *args,
            user: str = _user,
            password: str = _password,
            host: str = _host,
            port: str = _port,
            database: str = _database,
            **kwargs,
    ) -> None:
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

    def __del__(self) -> None:
        self._disconnect()

    @property
    def user(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._user

    @property
    def password(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._password

    @property
    def host(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._host

    @property
    def port(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._port

    @property
    def database(self) -> str:
        """
        Wrapper
        :return:
        """
        return self._database

    @property
    def connection(self):  # real signature unknown
        """
        Wrapper
        :return:
        """
        return self._connection

    @property
    def cursor(self):  # real signature unknown
        """
        Wrapper
        :return:
        """
        return self._cursor

    def _connect(self):  # real signature unknown
        """
        Wrapper
        Generalized
        :return:
        """
        return psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def _get_cursor(self):  # real signature unknown
        """
        Wrapper
        Generalized
        :return:
        """
        return self.connection.cursor()

    def _disconnect(self) -> None:
        """
        Wrapper
        Generalized
        :return:
        """
        self.cursor.close()
        self.connection.close()


class DatabaseStatic:
    """
    Just a Storage for Static Methods
    """

    @staticmethod
    def conv_float(val: str) -> float:
        """
        Converter
        field normalizer
        """
        if not val:
            return 0
        return float(val)

    @staticmethod
    def _get_csv_eans_records(csv_records: list) -> list[str]:
        """
        Unpack [ean] only from csv_records
        :param csv_records:
        :return:
        """
        return [
            ean for record in csv_records if (ean := record.get("ean", None))
        ]

    @staticmethod
    def _unpack_db_eans_records(
            db_eans_records: list[tuple[int]]
    ) -> list[str]:
        """
        Unpacks fetched records from DB
        To return [ean] only
        :param db_eans_records:
        :return:
        """
        return [str(db_record[0]) for db_record in db_eans_records]

    @staticmethod
    def compare_db_csv(
            db_eans_records: list[str], csv_eans_records: list[str]
    ) -> list[str]:
        """
        Returns missed records
        :param db_eans_records:
        :param csv_eans_records:
        :return:
        """
        return [
            csv_record
            for csv_record in csv_eans_records
            if csv_record not in db_eans_records
        ]

    @staticmethod
    def _columns_from_schema() -> list:
        """
        Return schema columns (id excluded)
        :return:
        """
        return [
            col
            for column in DatabaseBase.schema.strip().split(",")
            if (col := column.strip().split(" ")[0].strip()) != "id"
        ]


class Database(DatabaseBase, DatabaseStatic):
    """
    Database High-Level Interface
    """

    def _get_db_eans_records(self, table: str) -> list[str]:
        """
        Returns DB records
        [ean] only
        :param table:
        :return:
        """
        self._message("Comparing Records")
        query = sql.SQL(
            """
                                SELECT ean FROM {}
                                """
        ).format(sql.Identifier(table))
        self.execute_query(query)
        db_eans_records = self.fetch()
        return self._unpack_db_eans_records(db_eans_records)

    def add_records(self,
                    records_to_add: list,
                    csv_records: list,
                    table: str) -> None:
        """
        For Pipeline
        Includes normalization of float fields
        :param records_to_add:
        :param csv_records:
        :param table:
        :return:
        """
        self._message(f"Adding missing records - {len(records_to_add)}")
        for record in csv_records:
            record["price"] = self.conv_float(record["price"])
            record["old_price"] = self.conv_float(record["old_price"])
        records_to_add = [
            tuple(record.values())
            for record in csv_records
            if record.get("ean", "") in records_to_add
        ]
        columns = self._columns_from_schema()
        query = sql.SQL(
            """
                INSERT INTO {} ({})
                VALUES {}
                """
        ).format(
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(map(sql.Literal, records_to_add)),
        )
        self.execute_query(query)
        self.commit()
        self._message("Successfully added records")

    def compare_records(self, csv_records: list, table: str) -> list[str]:
        """
        Basically for Pipeline
        General method
        To find if there are new records in csv data
        :param csv_records:
        :param table:
        :return:
        """
        csv_eans_records = self._get_csv_eans_records(csv_records)
        db_eans_records = self._get_db_eans_records(table)
        return self.compare_db_csv(db_eans_records, csv_eans_records)

    def create_table(
            self, table: str, schema: str = DatabaseBase.schema
    ) -> None:
        """
        To create table from schema
        See default Schema
        :param table:
        :param schema:
        :return:
        """
        self._message(f"Creating table: {table}")
        query = sql.SQL(
            f"""
                CREATE TABLE {"{}"} (
                {schema}
                );
                """).format(sql.Identifier(table))
        self._message(query.as_string(self.connection))
        self.execute_query(query)
        self.commit()
        self._message(f"Table created: {table}")

    def drop_table(self, table: str) -> None:
        """
        For Future Use
        To Drop table
        Before execute requests user to accept
        :param table:
        :return:
        """
        print(f"Dropping table: {table}")
        query = sql.SQL(
            """
                DROP TABLE {};
                """
        ).format(sql.Identifier(table))
        if not input("Are you sure? y/yes").strip().lower() in ["y", "yes"]:
            print("Operation Aborted")
            return
        self.execute_query(query)
        self.commit()
        self._message(f"Table dropped: {table}")

    def table_exists(self, table: str) -> bool:
        """
        Checks if table exists
        :return:
        """
        self._message(f"Checking if Table {table} exists")
        self.execute_query(
            "select * from information_schema.tables where table_name=%s",
            (table,),
        )
        if result := bool(self.fetch()):
            self._message(f"Table exists: {table}")
        else:
            self._message(f"Table does not exists: {table}")
        return result

    def execute_query(self, query: str, q_args: Collection = None) -> None:
        """
        Wrapper
        :return:
        """
        self.cursor.execute(query, q_args)

    def fetch(self, how_many: int = 0) -> list[tuple[Any]]:
        """
        Unified Wrapper
        :return:
        """
        match how_many:
            case 0:
                return self.cursor.fetchall()
            case 1:
                return [self.cursor.fetchone()]
            case _:
                return self.cursor.fetchmany(how_many)

    def commit(self) -> None:
        """
        Wrapper
        :return:
        """
        self.connection.commit()
