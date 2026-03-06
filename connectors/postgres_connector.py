"""
connectors/postgres_connector.py
---------------------------------
PostgreSQL data extraction connector.

Establishes a connection to a PostgreSQL database, executes a provided SQL
query, and returns results as a pandas DataFrame. Provides descriptive error
messages for common connection and authentication failures.
"""

import pandas as pd


class PostgresConnector:
    """
    Stateless connector for extracting data from PostgreSQL databases.

    All interaction is handled through the single ``fetch`` class method,
    so no instance state is required.
    """

    @staticmethod
    def fetch(conn_config: dict, query: str) -> pd.DataFrame:
        """
        Execute a SQL query against a PostgreSQL database and return results.

        Parameters
        ----------
        conn_config : dict
            Connection parameters:
                - host     (str)           : Server hostname or IP address.
                - port     (int, optional) : Port number. Defaults to 5432.
                - database (str)           : Target database name.
                - user     (str)           : Authentication username.
                - password (str)           : Authentication password.
        query : str
            SQL statement to execute.

        Returns
        -------
        pd.DataFrame
            Query results.

        Raises
        ------
        ValueError
            If any required connection field is absent.
        RuntimeError
            If the connection attempt or query execution fails.

        Example
        -------
        >>> config = {
        ...     "host": "localhost", "port": 5432,
        ...     "database": "warehouse", "user": "analyst", "password": "s3cr3t"
        ... }
        >>> df = PostgresConnector.fetch(config, "SELECT * FROM dim_product LIMIT 10")
        """
        host = conn_config.get("host")
        port = conn_config.get("port", 5432)
        database = conn_config.get("database")
        user = conn_config.get("user")
        password = conn_config.get("password")

        missing = [k for k, v in {"host": host, "database": database, "user": user, "password": password}.items() if not v]
        if missing:
            raise ValueError(
                f"PostgreSQL connection config is missing required fields: {missing}\n"
                f"  Provided config keys: {list(conn_config.keys())}"
            )

        print(f"     🌐 Connecting to PostgreSQL: {host}:{port}/{database}")
        print(f"     👤 User: {user}")

        try:
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            )

            print("     ✅ PostgreSQL connection established")
            print("     🔍 Executing query...")

            df = pd.read_sql(query, conn)
            conn.close()

            print(f"     📊 Fetched {len(df)} rows across {len(df.columns)} columns")
            if df.empty:
                print("     ⚠️  Query returned zero rows — verify the SQL and filters")

            return df

        except psycopg2.Error as db_err:
            raise RuntimeError(
                f"PostgreSQL error while querying {host}:{port}/{database}\n"
                f"  Error type    : {type(db_err).__name__}\n"
                f"  Error message : {db_err}"
            ) from db_err

        except Exception as exc:
            raise RuntimeError(
                f"Unexpected error while querying PostgreSQL at {host}:{port}/{database}\n"
                f"  Error type    : {type(exc).__name__}\n"
                f"  Error message : {exc}"
            ) from exc
