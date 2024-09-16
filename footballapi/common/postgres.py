"""Connector and methods accessing Postgres"""
import psycopg2
from sqlalchemy import create_engine
import logging

class PostgresConnector():
    """
    Class for interacting with Postgres
    """
    def __init__(self, conn_string, table_name, schema_name):
        """
        Constructor for PostgresConnector

        :param url: url to access FootballAPI
        :param querystring: query string for FootballAPI call
        :param headers: headers for FootballAPI call
        """
        self.logger = logging.getLogger(__name__)
        self.conn_string = conn_string
        self.table_name = table_name
        self.schema_name = schema_name

    def write_df_to_postgres(self, df):
        db = create_engine(self.conn_string) 
        conn = db.connect()
        df.to_sql(self.table_name, con=conn, schema=self.schema_name, if_exists='replace', index=False) 
        conn1 = psycopg2.connect(self.conn_string) 
        conn1.autocommit = True
        conn.close()
        conn1.close()
        return True