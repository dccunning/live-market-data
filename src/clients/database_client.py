__author__ = 'Dimitri Cunning'

import os
import uuid
import math
import asyncpg
import psycopg2
from typing import List
from decimal import Decimal
from dotenv import load_dotenv
from datetime import datetime, date, time

load_dotenv()


class Database:
    def __init__(
            self,
            database: str = os.getenv("DATABASE_NAME"),
            user: str = os.getenv("DATABASE_USER"),
            password: str = os.getenv("DATABASE_PASSWORD"),
            host: str = os.getenv("DATABASE_HOME_HOST"),# if on_home_network() else os.getenv("DATABASE_AWAY_HOST"),
            port: str = os.getenv("DATABASE_PORT")
    ):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        print("DB CONFIG:", self.database, self.user, self.password, self.host, self.port)

    def __get_db_connection(self):
        """Database connection setup."""
        return psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    async def __aget_db_connection(self):
        """Database connection setup (async)."""
        return await asyncpg.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def run_query(self, query: str, params: tuple | List = None) -> List | None:
        """Return results for a query executed on the given database."""
        with self.__get_db_connection() as conn:
            with conn.cursor() as cursor:
                if params:
                    # If params is a list (bulk data) and the first element is a tuple/list, use execute_values.
                    if isinstance(params, list) and params and isinstance(params[0], (tuple, list)):
                        from psycopg2.extras import execute_values
                        execute_values(cursor, query, params)
                        return None
                    else:
                        cursor.execute(query, params)
                        return None
                else:
                    cursor.execute(query)
                data = cursor.fetchall()

                columns = [desc[0] for desc in cursor.description]
                json_list = [dict(zip(columns, [serialize_value(val) for val in row])) for row in data]
                return json_list

    async def arun_query(self, query: str, params: tuple | List = None) -> List | None:
        """Asynchronously return results for a query executed on the given database."""
        conn = await self.__aget_db_connection()
        try:
            if params:
                if isinstance(params, list) and params and isinstance(params[0], (tuple, list)):
                    await conn.executemany(query, params)  # Asyncpg equivalent of execute_values
                    return None
                else:
                    data = await conn.fetch(query, *params)
            else:
                data = await conn.fetch(query)

            json_list = [
                {k: serialize_value(v) for k, v in row.items()}
                for row in data
            ]

            return json_list
        finally:
            await conn.close()


def serialize_value(value):
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, (datetime, date, time)):
        return value.isoformat()
    elif isinstance(value, bytes):
        return memoryview(value)
    elif isinstance(value, uuid.UUID):
        return int(value)
    elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    elif isinstance(value, list):
        return [serialize_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    else:
        return value


DatabaseClient = Database()
