import os
import sys

import dotenv
import pandas as pd
from sqlalchemy import create_engine, text, Engine, Connection

dotenv.load_dotenv()

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = "ImagoTest"

# Mapping of database table names to their corresponding source CSV files
TABLES_MAP = {
    "Abrechnung_Positionen": "data/positions.csv",
    "Abrechnung_Rechnungen": "data/invoices.csv",
    "Abrechnung_Kunden": "data/customers.csv"
}


def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy engine for the MS SQL Server database.

        Returns:
            Engine: The SQLAlchemy database engine instance.
    """
    database_url = f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"Connecting to a database at '{database_url}'")

    return create_engine(database_url)


def read_csv(file_path: str, encoding: str) -> pd.DataFrame:
    """Reads a semicolon-separated CSV file into a pandas DataFrame.

        Handles 'NULL' strings as NaN values and uses the specified encoding.

        Args:
            file_path (str): The path to the input CSV file.
            encoding (str): The character encoding of the file (e.g., 'iso-8859-1').

        Returns:
            pd.DataFrame: A DataFrame containing the data read from the CSV file.
    """
    return pd.read_csv(
        file_path, 
        sep=";",
        na_values="NULL",
        keep_default_na=True,
        encoding=encoding
    )


def exec_insert_transaction(
        connection: Connection,
        data: pd.DataFrame,
        table_name: str,
        table_schema: str,
        truncate_table: bool
) -> None:
    """Executes DataFrame insertion within a single DB transaction.

        Handles optional table truncation and toggles IDENTITY_INSERT setting
        for the target table to allow insertion of explicit identity column values.

        Args:
            connection (Connection): An active SQLAlchemy database connection.
            data (pd.DataFrame): The pandas DataFrame containing data to insert.
            table_name (str): The name of the target database table.
            table_schema (str): The schema of the target database table.
            truncate_table (bool): If True, the table will be truncated before
                insertion.
    """
    with connection.begin():
        if truncate_table:
            print(f"Truncating table '{table_schema}.{table_name}'")
            connection.execute(
                text(f"TRUNCATE TABLE {table_schema}.{table_name};")
            )

        # Enable identity insert for the duration of the transaction block
        connection.execute(
            text(f"SET IDENTITY_INSERT {table_schema}.{table_name} ON;")
        )
        try:
            data.to_sql(
                table_name,
                connection,
                if_exists="append",
                index=False,
                schema=table_schema
            )
        except Exception as exc:
            raise exc
        finally:
            # Ensure IDENTITY_INSERT is turned off even if to_sql fails
            connection.execute(
                text(f"SET IDENTITY_INSERT {table_schema}.{table_name} OFF;")
            )


def insert_data(
        engine: Engine,
        file_path: str, 
        encoding: str,
        table_name: str,
        table_schema: str = "dbo",
        truncate_table: bool = True
) -> None:
    """Reads data from a CSV file and inserts it into a specified database table.

        Args:
            engine (Engine): The SQLAlchemy database engine to use for connection.
            file_path (str): The path to the source CSV file.
            encoding (str): The character encoding of the CSV file.
            table_name (str): The name of the target database table.
            table_schema (str, optional): The schema of the target database table.
                Defaults to "dbo".
            truncate_table (bool, optional): Whether to truncate the table before
                insertion. Defaults to True.
        """
    print(
        f"Inserting data from '{file_path}' into table '{table_schema}.{table_name}'"
    )
    df = read_csv(file_path, encoding)

    with engine.connect() as connection:
        exec_insert_transaction(
            connection, df, 
            table_name, 
            table_schema, 
            truncate_table
        )
    print(
        f"{len(df)} rows inserted into '{table_schema}.{table_name}' table successfully"
    )


def main() -> None:
    """Main function to orchestrate the data loading process.

        Sets up the database engine, iterates through the tables defined in
        `tables_map`, and calls `insert_data` for each table/file pair.
    """
    engine = None
    data_encoding = "iso-8859-1"  # Support for special German characters

    try:
        engine = get_db_engine()

        for table_name, file_path in TABLES_MAP.items():
            insert_data(engine, file_path, data_encoding, table_name)
    except Exception as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()
