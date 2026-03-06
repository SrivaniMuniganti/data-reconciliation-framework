"""
connectors/sqlserver_connector.py
----------------------------------
SQL Server / Azure SQL data extraction connector.

Establishes an ODBC connection to a SQL Server or Azure SQL database,
executes a provided SQL query, and returns results as a pandas DataFrame.

Requirements
------------
- pyodbc
- ODBC Driver 18 for SQL Server (or compatible) installed on the host system.
  Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
"""

import pandas as pd


class SqlServerConnector:
    """
    Stateless connector for extracting data from SQL Server / Azure SQL databases.

    All interaction is handled through the single ``fetch`` class method.
    """

    @staticmethod
    def fetch(conn_config: dict, query: str) -> pd.DataFrame:
        """
        Execute a SQL query against a SQL Server or Azure SQL database.

        Parameters
        ----------
        conn_config : dict
            Connection parameters:
                - host     (str)           : Server hostname (e.g. myserver.database.windows.net).
                - port     (int, optional) : Port number. Defaults to 1433.
                - database (str)           : Target database name.
                - user     (str)           : Authentication username.
                - password (str)           : Authentication password.
                - driver   (str, optional) : ODBC driver name.
                                             Defaults to ``ODBC Driver 18 for SQL Server``.
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

        Connection String
        -----------------
        Built as:
            DRIVER={<driver>};SERVER=<host>,<port>;DATABASE=<db>;UID=<user>;
            PWD=<pass>;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
        """
        host = conn_config.get("host")
        port = conn_config.get("port", 1433)
        database = conn_config.get("database")
        user = conn_config.get("user")
        password = conn_config.get("password")
        driver = conn_config.get("driver", "ODBC Driver 18 for SQL Server")

        missing = [k for k, v in {"host": host, "database": database, "user": user, "password": password}.items() if not v]
        if missing:
            raise ValueError(
                f"SQL Server connection config is missing required fields: {missing}\n"
                f"  Provided config keys: {list(conn_config.keys())}"
            )

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        print(f"     🌐 Connecting to SQL Server: {host}:{port}/{database}")
        print(f"     👤 User: {user}")
        print(f"     🚗 Driver: {driver}")

        try:
            import pyodbc
            conn = pyodbc.connect(conn_str)

            print("     ✅ SQL Server connection established")
            print("     🔍 Executing query...")

            df = pd.read_sql(query, conn)
            conn.close()

            print(f"     📊 Fetched {len(df)} rows across {len(df.columns)} columns")
            if df.empty:
                print("     ⚠️  Query returned zero rows — verify the SQL and filters")

            return df

        except pyodbc.Error as db_err:
            error_msg = str(db_err)
            if "Login failed" in error_msg or "authentication" in error_msg.lower():
                hint = "\n  💡 Check credentials. For Azure SQL the username format is: user@servername"
            elif "Cannot open server" in error_msg or "network" in error_msg.lower():
                hint = "\n  💡 Check the server hostname and firewall rules. Ensure the client IP is allowed."
            elif "driver" in error_msg.lower():
                hint = f"\n  💡 ODBC driver '{driver}' may not be installed. Download from Microsoft."
            else:
                hint = ""

            raise RuntimeError(
                f"SQL Server error while querying {host}:{port}/{database}\n"
                f"  Error type    : {type(db_err).__name__}\n"
                f"  Error message : {error_msg}"
                f"{hint}"
            ) from db_err

        except Exception as exc:
            raise RuntimeError(
                f"Unexpected error while querying SQL Server at {host}:{port}/{database}\n"
                f"  Error type    : {type(exc).__name__}\n"
                f"  Error message : {exc}"
            ) from exc
