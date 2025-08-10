from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal


def make_engine(server: str, database: str, username: str, password: str):
    params = quote_plus(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def write_weather(
        df: pd.DataFrame,
        engine,
        table: str = "WeatherDataClean",
        if_exists: Literal["fail", "replace", "append"] = "replace"
) -> None:
    df.to_sql(table, con=engine, if_exists=if_exists, index=False)
