import os
import sys
import pyodbc

driver = os.getenv("AZURE_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
server = os.environ["AZURE_SQL_SERVER"].strip()
host = server if server.lower().endswith(".database.windows.net") else f"{server}.database.windows.net"
db = os.environ.get("AZURE_SQL_DATABASE", "master")
uid = os.environ["AZURE_SQL_USERNAME"]
pwd = os.environ["AZURE_SQL_PASSWORD"]

dsn = (
    f"Driver={{{driver}}};"
    f"Server=tcp:{host},1433;"
    f"Database={db};"
    f"Uid={uid};"
    f"Pwd={pwd};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)

print("DSN (redacted):", dsn.replace(pwd, "***"))

try:
    with pyodbc.connect(dsn) as cn:
        cur = cn.cursor()
        cur.execute("SELECT DB_NAME()")
        print("Connected OK; DB_NAME():", cur.fetchone()[0])
except pyodbc.Error as e:
    print("ODBC error:", e)
    sys.exit(1)
