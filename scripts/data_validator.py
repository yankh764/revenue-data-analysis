import sys

import pandas as pd

PLACEHOLDER_MEDIA_ID = 100000000
# Mapping of data files names to their corresponding source CSV files
TABLES_MAP = {
    "positions": "data/positions.csv",
    "invoices": "data/invoices.csv",
    "customers": "data/customers.csv"
}


def read_csv(file_path: str, encoding: str) -> pd.DataFrame:
    """Reads a semicolon-separated CSV file into a pandas DataFrame.

        Handles 'NULL' strings as NaN values and uses the specified encoding.

        Args:
            file_path: The path to the input CSV file.
            encoding: The character encoding of the file (e.g., 'iso-8859-1').

        Returns:
            A pandas DataFrame containing the data read from the CSV file.
    """
    return pd.read_csv(
        file_path,
        sep=";",
        na_values="NULL",
        keep_default_na=True,
        encoding=encoding
    )


def perform_invoice_checks(
        invoices_df: pd.DataFrame,
        valid_customer_ids: set[int]
) -> list[str]:
    """Performs validation checks on the invoices DataFrame.

    Checks for:
    - Missing mandatory fields ('KdNr', 'ReNummer').
    - 'KdNr' values that do not exist in the provided set of valid customer IDs.

    Args:
        invoices_df: DataFrame containing invoice data.
        valid_customer_ids: A set of valid customer IDs ('KdNr') for integrity checks.

    Returns:
        A list of strings, where each string describes a specific validation issue found.
    """
    issues_list = []

    # Rule I1: Missing required fields
    for idx, row in invoices_df[invoices_df["KdNr"].isnull() | invoices_df["ReNummer"].isnull()].iterrows():
        issues_list.append(
            f"Invoice (ID:{row.get('ReNummer', 'NaN')}, Index:{idx}): Missing KdNr or ReNummer"
        )

    # Rule I2: Invalid Customer reference (KdNr)
    inv_check_kdnr = invoices_df[invoices_df["KdNr"].notnull()]
    invalid_kdnr_inv = inv_check_kdnr[
        ~pd.to_numeric(inv_check_kdnr["KdNr"], errors="coerce").isin(valid_customer_ids)
    ]
    for idx, row in invalid_kdnr_inv.iterrows():
        issues_list.append(
            f"Invoice {row.get('ReNummer', 'NaN')}: Invalid KdNr {row['KdNr']:.0f}"
        )
    return issues_list


def perform_position_checks(
        positions_df: pd.DataFrame,
        valid_invoice_ids: set[int],
        valid_customer_ids: set[int]
) -> list[str]:
    """Performs validation checks on the positions DataFrame.

    Checks for:
    - Missing 'ReId' (invoice reference).
    - 'ReId' values that do not exist in the provided set of valid invoice IDs.
    - 'KdNr' values that do not exist in the provided set of valid customer IDs.
    - Usage of the placeholder media ID ('Bildnummer').

    Args:
        positions_df: DataFrame containing position (line item) data.
        valid_invoice_ids: A set of valid invoice IDs ('ReNummer') for integrity checks.
        valid_customer_ids: A set of valid customer IDs ('KdNr') for integrity checks.

    Returns:
        A list of strings describing validation issues or warnings found.
    """
    issues_list = []

    # Rule P1: Missing Invoice reference (ReId)
    for idx, row in positions_df[positions_df["ReId"].isnull()].iterrows():
        issues_list.append(
            f"Position (ID:{row.get('id', 'NaN')}, Index:{idx}): Missing ReId"
        )

    # Rule P2: Invalid Invoice reference (ReId)
    pos_check_reid = positions_df[positions_df["ReId"].notnull()]
    invalid_reid_pos = pos_check_reid[
        ~pd.to_numeric(pos_check_reid["ReId"], errors="coerce").isin(valid_invoice_ids)
    ]
    for idx, row in invalid_reid_pos.iterrows():
        issues_list.append(
            f"Position {row.get('id', 'NaN')}: Invalid ReId {row['ReId']:.0f}"
        )

    # Rule P3: Invalid Customer reference (KdNr)
    pos_check_kdnr = positions_df[positions_df["KdNr"].notnull()]
    invalid_kdnr_pos = pos_check_kdnr[
        ~pd.to_numeric(pos_check_kdnr["KdNr"], errors="coerce").isin(valid_customer_ids)
    ]
    for idx, row in invalid_kdnr_pos.iterrows():
        issues_list.append(
            f"Position {row.get('id', 'NaN')}: Invalid KdNr {row['KdNr']:.0f}"
        )

    # Rule P4: Placeholder Media ID (Warning)
    placeholders = positions_df[
        pd.to_numeric(positions_df["Bildnummer"], errors="coerce") == PLACEHOLDER_MEDIA_ID
    ]
    for idx, row in placeholders.iterrows():
        issues_list.append(
            f"Position {row.get('id', 'NaN')}: Uses placeholder Bildnummer"
        )
    return issues_list


def perform_validation_checks(
        invoices_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        customers_df: pd.DataFrame
) -> list[str]:
    """Orchestrates the validation process across all related DataFrames.

    Builds sets of valid IDs from customer and invoice data, then calls
    specific validation functions for invoices and positions to aggregate
    all identified issues.

    Args:
        invoices_df: DataFrame with invoice data.
        positions_df: DataFrame with position data.
        customers_df: DataFrame with customer data.

    Returns:
        A list of strings, where each string describes a specific validation
        issue found across all checks. Returns an empty list if no issues
        are found.
    """
    all_issues = []
    # --- Prepare lookup sets ---
    valid_customer_ids = set(
        pd.to_numeric(customers_df["Kdnr"], errors="coerce").dropna().astype(int)
    )
    valid_invoice_ids = set(
        pd.to_numeric(invoices_df["ReNummer"], errors="coerce").dropna().astype(int)
    )

    invoice_issues = perform_invoice_checks(invoices_df, valid_customer_ids)
    all_issues.extend(invoice_issues)
    position_issues = perform_position_checks(positions_df, valid_invoice_ids, valid_customer_ids)
    all_issues.extend(position_issues)

    return all_issues


def print_discovered_issues(issues: list[str], top: int = 10) -> None:
    """Prints a summary and a sample of discovered validation issues.

    Outputs the total number of issues found. If issues exist, it prints
    up to a specified number ('top') of the individual issue messages,
    each prefixed with its rank number. If no issues are found, it prints
    a corresponding message.

    Args:
        issues: A list of strings, where each string is a validation issue message.
        top: The maximum number of individual issue messages to print. Defaults to 10.
    """
    if issues:
        actual_top = min(top, len(issues))
        print(f"Validation identified {len(issues)} potential issues, printing top {actual_top}:")
        for i in range(actual_top):
            print(f"#{i + 1}: {issues[i]}")
    else:
        print("No issues identified by simple validation.")


def main() -> None:
    """Main function to orchestrate the data validation process.

    Reads customer, invoice, and position data from CSV files defined
    in TABLES_MAP, then runs validation checks across them using
    perform_validation_checks(). Handles file reading errors and other
    exceptions during the process.
    """
    data_encoding = "iso-8859-1"  # Support for special German characters

    try:
        print("Starting data validation...")
        invoices_df = read_csv(TABLES_MAP["invoices"], data_encoding)
        print(f"Read {len(invoices_df)} rows from '{TABLES_MAP['invoices']}'")

        positions_df = read_csv(TABLES_MAP["positions"], data_encoding)
        print(f"Read {len(positions_df)} rows from '{TABLES_MAP['positions']}'")

        customers_df = read_csv(TABLES_MAP["customers"], data_encoding)
        print(f"Read {len(customers_df)} rows from '{TABLES_MAP['customers']}'")

        print("Performing validation checks...")
        results = perform_validation_checks(invoices_df, positions_df, customers_df)
        print_discovered_issues(results)
    except Exception as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
