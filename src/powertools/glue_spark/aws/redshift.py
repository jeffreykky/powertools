import redshift_connector
from ..log import GlueLogger as logger


connection = None

class RedshiftUtils:

    def connect(self, host, port, database, user, password) -> redshift_connector.Connection:
        """
        Establish a connection to the Redshift cluster.
        """
        try:
            self.conn = redshift_connector.connect(host=host, port=port, database=database, user=user, password=password)
            logger.info("Connection to Redshift established.")
            return self.conn
        except redshift_connector.Error as e:
            logger.error(f"Error connecting to Redshift: {e}")
            self.conn = None

    def get_conn(self, conn) -> redshift_connector.Connection:
        conn = conn | self.conn
        if not conn:
            logger.error("No active connection to Redshift.")
            raise redshift_connector.Error("No active connection to Redshift")
        return conn
        
    def close_connection(self):
        """
        Close the connection to the Redshift cluster.
        """
        if self.conn:
            self.conn.close()
            logger.info("Connection to Redshift closed.")

    def execute_query(self, query, conn = None):
        """
        Execute a SQL query on the Redshift cluster.

        :param query: SQL query string
        :return: Query result (if applicable)
        """
        conn = self.get_conn(conn)

        try:
            cursor = conn.cursor()
            cursor.execute(query)
            if cursor.description:
                # Fetch results if the query returns data
                result = cursor.fetchall()
                cursor.close()
                return result
            cursor.close()
            conn.commit()
            logger.info("Query executed successfully.")
            return None
        except redshift_connector.Error as e:
            logger.error(f"Error executing query: {e}")
            return None

    def create_table(self, table_name, schema, conn = None):
        """
        Create a new table in the Redshift cluster.

        :param table_name: Name of the new table
        :param schema: Schema definition (e.g., "id INT, name VARCHAR(50)")
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        self.execute_query(query, conn=conn)
        logger.info(f"Table {table_name} created.")

    def drop_table(self, table_name, conn=None):
        """
        Drop a table from the Redshift cluster.

        :param table_name: Name of the table to drop
        """
        query = f"DROP TABLE IF EXISTS {table_name};"
        self.execute_query(query, conn=conn)
        logger.info(f"Table {table_name} dropped.")

    def insert_data(self, table_name, columns, values, conn=None):
        """
        Insert data into a table in the Redshift cluster.

        :param table_name: Name of the table
        :param columns: List of column names
        :param values: List of values to insert
        """
        columns_str = ", ".join(columns)
        values_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
        self.execute_query(query, conn=conn)
        logger.info(f"Data inserted into table {table_name}.")

    def select_data(self, table_name, columns='*', where_clause=None, conn=None):
        """
        Select data from a table in the Redshift cluster.

        :param table_name: Name of the table
        :param columns: List of columns to select (default is all columns)
        :param where_clause: Optional WHERE clause to filter the data
        :return: Query result
        """
        columns_str = ", ".join(columns) if isinstance(columns, list) else columns
        query = f"SELECT {columns_str} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += ";"
        return self.execute_query(query, conn=conn)
