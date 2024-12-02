# %%
from io import StringIO
import psycopg2
import numpy as np
import pandas as pd
import pyodbc

param_dic = {
    "host": "*********",
    "database": "*********",
    "user": "*********",
    "password": "*********",
}

# Make connection function for ms sql database using pyodbc
def connect_mssql(param_dic):
    conn = None
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER="
            + param_dic["host"]
            + ";"
            "DATABASE="
            + param_dic["database"]
            + ";"
            "UID="
            + param_dic["user"]
            + ";"
            "PWD="
            + param_dic["password"]
            + ";"
        )
        print("Connection successful")
    except Exception as e:
        print(f"Error: {e}")
    return conn

def connect(params_dic):
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

# Function to append or overwrite or drop data in the table using the copy_from_stringio function and automate incrementing the id column
def copy_from_stringio_auto_increment(conn, df, table, append=True):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # Check if the table exists
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM sys.tables WHERE name = '{table}'")
    exists = cursor.fetchone()[0]
    if exists:
        if append:
            print(f"Table {table} already exists. Appending data.")
            cursor.execute(f"SELECT MAX(id) FROM {table}")
            max_id = cursor.fetchone()[0]
            if max_id is None:
                max_id = 0
            df.index += max_id + 1
        else:
            cursor.execute(f"DROP TABLE {table}")
            print(f"Table {table} already exists. Dropping it and creating a new one.")
    # Dynamically create table schema
    columns = ", ".join(
        [
            f"{col} TEXT"
            if df[col].dtype == "object"
            else f"{col} FLOAT"
            if df[col].dtype == "float64"
            else f"{col} INT"
            for col in df.columns
        ]
    )
    print(columns)
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        {columns}
    )
    """)
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label="id", header=False, sep="|")
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep="|")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

# Write function for bulk load to ms sql database, using connection from connect_mssql function
def bulk_load_mssql(conn, df, table):
    cursor = conn.cursor()
    # Check if the table exists
    cursor.execute(f"SELECT 1 FROM sys.tables WHERE name = '{table}'")
    exists = cursor.fetchone()[0]
    if exists:
        print(f"Table {table} already exists. Dropping it and creating a new one.")
        cursor.execute(f"DROP TABLE {table}")
    # Dynamically create table schema
    columns = ", ".join(
        [
            f"{col} NVARCHAR(MAX)"
            if df[col].dtype == "object"
            else f"{col} FLOAT"
            if df[col].dtype == "float64"
            else f"{col} INT"
            for col in df.columns
        ]
    )
    print(columns)
    cursor.execute(f"""
    CREATE TABLE {table} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        {columns}
    )
    """)
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="|")
    buffer.seek(0)
    try:
        cursor.copy_from(buffer, table, sep="|")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

# Run the execute_many strategy
conn = connect(param_dic)

df = pd.read_csv("dummy_data.csv", sep="|")
df.drop(columns=["id"], inplace=True)
# print(df.dtypes)

copy_from_stringio_auto_increment(conn, df, "test_data_2")