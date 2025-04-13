# Revenue Data Pipeline Improvement Proposal
## Introduction
After analyzing the revenue data, I've identified several issues affecting accurate revenue attribution and payment tracking. 
This document outlines a straightforward solution to address these problems while keeping the implementation realistic.

## Current Issues Summary
From my analysis, three key issues were identified:

1. **Missing Payment Information**: 18,011 position records linked to invoices without payment data
2. **Content Attribution Problems**: €1.3M revenue attributed to placeholder media ID (100000000)
3. **Data Integrity Issues**: 2 invoices with no associated positions

## Proposed Pipeline Improvements
### 1. Enhanced Data Model
To address the identified issues without major changes to the existing schema, I recommend adding a dedicated data quality tracking table. 
This non-invasive approach allows us to monitor and resolve data issues over time while maintaining compatibility with existing systems:
```sql
-- New table to track data quality issues
CREATE TABLE [Abrechnung_Data_Quality] (
    [id] [int] IDENTITY(1,1) NOT NULL,
    [TableName] [varchar](50) NOT NULL,
    [RecordId] [int] NOT NULL,
    [IssueType] [varchar](50) NOT NULL, -- e.g., 'missing_payment', 'placeholder_media', 'empty_invoice'
    [IssueDate] [datetime] NOT NULL DEFAULT GETDATE(),
    [ResolvedDate] [datetime] NULL,
    [Notes] [varchar](255) NULL,
    CONSTRAINT [PK_Abrechnung_Data_Quality] PRIMARY KEY CLUSTERED ([id] ASC)
);
```

### 2. Upstream Data Validation
To prevent bad data from entering the system in the first place, I propose implementing a Python validation script that runs before data is loaded into the database:
```python
# Python snippet illustrating core data validation checks
import pandas as pd

def perform_validation_checks(invoices_df, positions_df, customers_df):
    issues_list = []

    # --- Prepare lookup sets ---
    valid_customer_ids = set(pd.to_numeric(customers_df["Kdnr"]).dropna().astype(int))
    valid_invoice_ids = set(pd.to_numeric(invoices_df["ReNummer"]).dropna().astype(int))

    # --- Invoice Checks ---
    # Rule I1: Missing required fields
    for idx, row in invoices_df[invoices_df["KdNr"].isnull() | invoices_df["ReNummer"].isnull()].iterrows():
        issues_list.append(f"Invoice (ID:{row.get('ReNummer', 'NaN')}, Index:{idx}): Missing KdNr or ReNummer.")

    # Rule I2: Invalid Customer reference (KdNr)
    inv_check_kdnr = invoices_df[invoices_df["KdNr"].notnull()]
    invalid_kdnr_inv = inv_check_kdnr[~pd.to_numeric(inv_check_kdnr["KdNr"]).isin(valid_customer_ids)]
    for idx, row in invalid_kdnr_inv.iterrows():
         issues_list.append(f"Invoice {row.get('ReNummer', 'NaN')}: Invalid KdNr {row['KdNr']}.")

    # --- Position Checks ---
    # Rule P1: Missing Invoice reference (ReId)
    for idx, row in positions_df[positions_df["ReId"].isnull()].iterrows():
        issues_list.append(f"Position (ID:{row.get('id', 'NaN')}, Index:{idx}): Missing ReId.")

    # Rule P2: Invalid Invoice reference (ReId)
    pos_check_reid = positions_df[positions_df["ReId"].notnull()]
    invalid_reid_pos = pos_check_reid[~pd.to_numeric(pos_check_reid["ReId"]).isin(valid_invoice_ids)]
    for idx, row in invalid_reid_pos.iterrows():
         issues_list.append(f"Position {row.get('id', 'NaN')}: Invalid ReId {row['ReId']}.")

    # Rule P3: Invalid Customer reference (KdNr)
    pos_check_kdnr = positions_df[positions_df["KdNr"].notnull()]
    invalid_kdnr_pos = pos_check_kdnr[~pd.to_numeric(pos_check_kdnr['KdNr']).isin(valid_customer_ids)]
    for idx, row in invalid_kdnr_pos.iterrows():
         issues_list.append(f"Position {row.get('id', 'NaN')}: Invalid KdNr {row['KdNr']}.")

    # Rule P4: Placeholder Media ID (Warning)
    placeholders = positions_df[pd.to_numeric(positions_df["Bildnummer"]) == 100000000]
    for idx, row in placeholders.iterrows():
        issues_list.append(f"Position {row.get('id', 'NaN')}: Uses placeholder Bildnummer.")

    if issues_list:
        print(f"Validation identified {len(issues_list)} potential issues.")
    else:
        print("No issues identified by simple validation.")
```

### 3. ETL Process Improvements
To improve the ETL process, my approach adds targeted quality checks to the existing process that identify and log three key issues, to the newly created table: 
missing payments, placeholder media IDs, and empty invoices. This provides visibility into data problems without disrupting the current pipeline flow:
```python
# Python snippet illustrating data quality checks
def run_data_quality_checks(connection):
    # 1. Check for missing payment information
    missing_payment_positions = connection.execute("""
        SELECT AP.id, AP.ReId
        FROM Abrechnung_Positionen AP
        JOIN Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
        WHERE AR.Zahlungsdatum IS NULL OR AR.ZahlungsbetragBrutto IS NULL
    """).fetchall()
    
    # Log missing payment issues
    for position in missing_payment_positions:
        connection.execute("""
            INSERT INTO Abrechnung_Data_Quality 
            (TableName, RecordId, IssueType, Notes)
            VALUES ('Abrechnung_Positionen', ?, 'missing_payment', 'Position linked to invoice without payment data')
        """, [position.id])
    
    # 2. Check for placeholder media
    placeholder_positions = connection.execute("""
        SELECT id, ReId 
        FROM Abrechnung_Positionen
        WHERE Bildnummer = 100000000
    """).fetchall()
    
    # Log placeholder media issues
    for position in placeholder_positions:
        connection.execute("""
            INSERT INTO Abrechnung_Data_Quality 
            (TableName, RecordId, IssueType, Notes)
            VALUES ('Abrechnung_Positionen', ?, 'placeholder_media', 'Position using placeholder media ID')
        """, [position.id])
    
    # 3. Check for invoices without positions
    empty_invoices = connection.execute("""
        SELECT AR.ReNummer
        FROM Abrechnung_Rechnungen AR
        LEFT JOIN Abrechnung_Positionen AP ON AP.ReId = AR.ReNummer
        WHERE AP.id IS NULL
    """).fetchall()
    
    # Log empty invoice issues
    for invoice in empty_invoices:
        connection.execute("""
            INSERT INTO Abrechnung_Data_Quality 
            (TableName, RecordId, IssueType, Notes)
            VALUES ('Abrechnung_Rechnungen', ?, 'empty_invoice', 'Invoice has no associated positions')
        """, [invoice.ReNummer])
```

### 4. Improved Views for Reporting
To address data quality issues without modifying the core data structure, I propose creating database views that intelligently handle problematic cases. 
These views will serve as a reliable data source for Tableau dashboards while maintaining consistency with existing reports:
```sql
-- View for revenue reporting that handles missing payments
CREATE VIEW [vw_Revenue_Complete] AS
SELECT 
    AP.id AS PositionId,
    AP.ReId AS InvoiceId,
    AP.KdNr AS CustomerId,
    AP.Nettobetrag AS NetAmount,
    AP.Bildnummer AS MediaId,
    AP.VerDatum AS PublicationDate,
    AR.ReDatum AS InvoiceDate,
    AR.Zahlungsdatum AS PaymentDate,
    -- If payment data is missing, mark as 'Pending'
    CASE 
        WHEN AR.Zahlungsdatum IS NULL THEN 'Pending'
        ELSE 'Paid'
    END AS PaymentStatus,
    -- Flag placeholder media for special handling
    CASE 
        WHEN AP.Bildnummer = 100000000 THEN 'Placeholder'
        ELSE 'Actual'
    END AS MediaType,
    K.Verlagsname AS CustomerName,
    K.Region AS CustomerRegion
FROM 
    Abrechnung_Positionen AP
JOIN 
    Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
LEFT JOIN 
    Abrechnung_Kunden K ON AP.KdNr = K.Kdnr;
```

## Solution Diagram
```
┌─────────────────────┐
│                     │
│   Legacy Systems    │
│  (Access/Excel/Word)│
│                     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│                     │
│   Python Data       │
│   Validation Script │
│                     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌─────────────────────┐
│                     │     │                     │
│   SSIS Pipeline     │────►│  Data Quality       │
│                     │     │  Checks             │
│                     │     │                     │
└──────────┬──────────┘     └─────────┬───────────┘
           │                          │
           ▼                          ▼
┌─────────────────────┐     ┌─────────────────────┐
│                     │     │                     │
│  SQL Server Tables  │     │ Abrechnung_Data_    │
│  - Rechnungen       │     │ Quality Table       │
│  - Positionen       │     │                     │
│  - Kunden           │     │                     │
│                     │     │                     │
└──────────┬──────────┘     └─────────┬───────────┘
           │                          │
           ▼                          ▼
┌─────────────────────────────────────────────────┐
│                                                 │
│             Enhanced Database Views             │
│             (vw_Revenue_Complete)               │
│                                                 │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│                                                 │
│         IMAGO_Matrix_Metrics_2025               │
│         Tableau Dashboard                       │
│                                                 │
└─────────────────────────────────────────────────┘
```

## Assumptions
In developing this proposal, I've made the following assumptions:

1. The existing database structure should remain intact to avoid disrupting current operations
2. We have the ability to run Python scripts as part of the ETL process
3. The data typically comes from CSV files before being loaded into the database

## Impact on Downstream Reporting
Implementing these changes will impact downstream reporting in the following ways:

1. **Improved Data Quality**: Upstream validation will prevent many issues from entering the system.
2. **Visibility of Data Quality Issues**: The new data quality table allows creating reports that highlight problematic data.
3. **Better Payment Visibility**: The enhanced view explicitly labels pending vs. paid invoices, rather than showing NULL values.
4. **Placeholder Media Identification**: By flagging placeholder media in the view, reports can easily filter or highlight this revenue separately.
5. **Minimal Disruption**: The approach uses views rather than table changes, meaning existing reports continue functioning with enhanced data.

## Business Conversations
To implement these changes effectively, I'd initiate the following conversations:

### With Backoffice Team:
1. Establish whether validation should be strict (block bad data) or advisory (warn but allow)
2. Discuss the process for handling placeholder media IDs - when are they legitimate vs. when they should be resolved?

### With Finance Team:
1. Confirm the interpretation of missing payment data e.g. is "Pending" the right status?

### With BI Team:
1. Discuss how to incorporate the enhanced view into the IMAGO_Matrix_Metrics_2025 dashboard
2. Consider creating a simple data quality dashboard for monitoring issues

This approach provides a balanced solution that addresses both upstream validation (preventing bad data) and 
downstream reporting (handling existing issues), without requiring major changes to the existing infrastructure.