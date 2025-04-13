# Revenue Data Analysis Findings
## Introduction
This document outlines my findings from analyzing the invoice and position data provided as part of the data pipeline challenge. 
The analysis focuses on identifying specific issues with revenue attribution in the reporting system.

## Dataset Overview
I analyzed three connected datasets representing the invoice system:

- `invoices.csv` - Main invoice records (Abrechnung_Rechnungen)
- `positions.csv` - Line items for each invoice (Abrechnung_Positionen)
- `customers.csv` - Customer information lookup table (Abrechnung_Kunden)

## Analysis Approach
I used SQL queries against the relational tables to identify three key issues mentioned in the challenge requirements:

1. Positions linked to invoices with missing payment information
2. Revenue attributed to placeholder media IDs
3. Invoices with no associated positions

The full SQL analysis query can be found [here](../sql/analysis.sql).

## Key Findings
### 1. Missing Payment Information
**Finding:** 18,011 position records are linked to invoices that have missing payment information.

This means a significant number of line items in the system are associated with invoices where either 
the payment date (`Zahlungsdatum`) or payment amount (`ZahlungsbetragBrutto`) is not recorded.

**SQL Query:**
```sql
SELECT COUNT(*) AS count_positions_missing_payment
FROM Abrechnung_Positionen AP
JOIN Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
WHERE AR.Zahlungsdatum IS NULL OR AR.ZahlungsbetragBrutto IS NULL;
```

### 2. Placeholder Media Revenue
**Finding:** €1,319,897.91 of revenue is attributed to the placeholder media ID '100000000'.

This substantial amount indicates that a significant portion of revenue is not being properly linked 
to actual media content, making accurate content performance analysis impossible.

**SQL Query:**
```sql
SELECT SUM(Nettobetrag) AS sum_placeholder_media_revenue
FROM Abrechnung_Positionen
WHERE Bildnummer = 100000000;
```

### 3. Invoices Without Positions
**Finding:** 2 invoices in the system have no attached position records.

While this is a relatively small number, these "empty" invoices represent a data integrity issue 
where revenue might be recorded at the invoice level but not properly broken down into component line items.

**SQL Query:**
```sql
SELECT COUNT(*) AS count_invoices_missing_positions
FROM Abrechnung_Rechnungen AR
LEFT JOIN Abrechnung_Positionen AP ON AP.ReId = AR.ReNummer
WHERE AP.id IS NULL;
```

## Implications
These findings point to several data quality issues in the current revenue pipeline:

1. **Payment Tracking Gaps**: The large number of positions with missing payment data suggests the payment reconciliation process isn't being captured completely in the data flow.
2. **Content Attribution Problems**: Over €1.3M in revenue can't be properly attributed to specific media content, which likely impacts content performance metrics.
3. **Structural Integrity Issues**: The existence of invoices without line items, while small in number, indicates a breakdown in the expected relational structure of the data.

These issues likely contribute to the reported problems with revenue attribution and missing payments in dashboards, as mentioned in the challenge description.