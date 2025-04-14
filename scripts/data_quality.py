import os
import sys

import dotenv
from sqlalchemy import create_engine, text, Engine, Connection

dotenv.load_dotenv()

DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = "ImagoTest"

PLACEHOLDER_MEDIA_ID = 100000000


def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy engine for the MS SQL Server database.

        Returns:
            Engine: The SQLAlchemy database engine instance.
    """
    database_url = f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"Connecting to a database at '{database_url}'")

    return create_engine(database_url)


def insert_bulk_data(
        connection: Connection,
        bulk_data: list[dict[str, any]]
) -> None:
    """Performs a batch insert into the data quality table.

    Uses SQLAlchemy's execute method with a list of dictionaries to
    efficiently insert multiple rows in a single operation.

    Args:
        connection: An active SQLAlchemy Connection object with a transaction.
        bulk_data: A list where each item is a dictionary representing a row
                   to insert. Keys must match named parameters in the SQL.
    """
    connection.execute(text("""
        INSERT INTO Abrechnung_Data_Quality 
        (TableName, RecordId, IssueType, Notes)
        VALUES (:TableName, :RecordId, :IssueType, :Notes)
    """), bulk_data)


def perform_payment_quality_check(connection: Connection) -> int:
    """Finds positions linked to invoices missing payment data and logs them.

    Queries for positions where the corresponding invoice lacks a payment date
    or amount, formats these findings, inserts them into the quality table
    using insert_bulk_data, and returns the count of issues logged.

    Args:
        connection: An active SQLAlchemy Connection object with a transaction.

    Returns:
        The number of 'missing_payment' issues found and logged.
    """
    missing_payment_positions = connection.execute(text("""
        SELECT AP.id
        FROM Abrechnung_Positionen AP
        JOIN Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
        WHERE AR.Zahlungsdatum IS NULL OR AR.ZahlungsbetragBrutto IS NULL
    """)).fetchall()

    bulk_data = [
        {
            "TableName": "Abrechnung_Positionen",
            "RecordId": position.id,
            "IssueType": "missing_payment",
            "Notes": "Position linked to invoice without payment data"
        }
        for position in missing_payment_positions
    ]
    if bulk_data:
        insert_bulk_data(connection, bulk_data)
    return len(bulk_data)


def perform_media_quality_check(connection: Connection) -> int:
    """Finds positions using the placeholder media ID and logs them.

    Queries for positions using the defined PLACEHOLDER_MEDIA_ID, formats
    these findings, inserts them into the quality table using
    insert_bulk_data, and returns the count of issues logged.

    Args:
        connection: An active SQLAlchemy Connection object with a transaction.

    Returns:
        The number of 'placeholder_media' issues found and logged.
    """
    placeholder_positions = connection.execute(text("""
        SELECT id 
        FROM Abrechnung_Positionen
        WHERE Bildnummer = :media_id
    """), {"media_id": PLACEHOLDER_MEDIA_ID}).fetchall()
    bulk_data = [
        {
            "TableName": "Abrechnung_Positionen",
            "RecordId": position.id,
            "IssueType": "placeholder_media",
            "Notes": "Position using placeholder media ID"
        }
        for position in placeholder_positions
    ]
    if bulk_data:
        insert_bulk_data(connection, bulk_data)
    return len(bulk_data)


def perform_invoices_quality_checks(connection: Connection) -> int:
    """Finds invoices that have no associated positions and logs them.

    Queries for invoices with no matching records in the positions table,
    formats these findings, inserts them into the quality table using
    insert_bulk_data, and returns the count of issues logged.

    Args:
        connection: An active SQLAlchemy Connection object with a transaction.

    Returns:
        The number of 'empty_invoice' issues found and logged.
    """
    empty_invoices = connection.execute(text("""
        SELECT AR.ReNummer
        FROM Abrechnung_Rechnungen AR
        LEFT JOIN Abrechnung_Positionen AP ON AR.ReNummer = AP.ReId
        WHERE AP.id IS NULL
    """)).fetchall()

    bulk_data = [
        {
            "TableName": "Abrechnung_Rechnungen",
            "RecordId": invoice.ReNummer,
            "IssueType": "empty_invoice",
            "Notes": "Invoice has no associated positions"
        }
        for invoice in empty_invoices
    ]
    if bulk_data:
        insert_bulk_data(connection, bulk_data)
    return len(bulk_data)


def perform_data_quality_checks(connection: Connection) -> None:
    """Orchestrates the data quality checks and logging process.

    Calls specific functions to check for different types of data quality issues
    (missing payments, placeholder media, empty invoices). Prints status messages
    and a final summary of the total number of issues logged across all checks.

    Args:
        connection: An active SQLAlchemy Connection object with a transaction.
    """
    logs_count = 0

    print("Performing data quality checks:")

    print("Checking payment data...")
    logs_count += perform_payment_quality_check(connection)
    print("Checking media data...")
    logs_count += perform_media_quality_check(connection)
    print("Checking invoice data...")
    logs_count += perform_invoices_quality_checks(connection)

    print(f"Finished data quality check process, {logs_count} records were logged")


def main() -> None:
    """Main execution function.

    Sets up the database engine, manages a transaction using engine.begin(),
    calls the primary data quality check function within the transaction,
    handles potential errors, and ensures the engine is disposed of.
    """
    engine = None

    try:
        engine = get_db_engine()

        with engine.begin() as connection:
            perform_data_quality_checks(connection)
    except Exception as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    main()
