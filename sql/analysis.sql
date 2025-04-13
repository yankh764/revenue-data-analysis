-- Switch to the IMAGO database
USE ImagoTest;
GO

-- Execute analysis query
WITH Faulty_Positions AS (
    SELECT COUNT(*) AS count_positions_missing_payment
    FROM Abrechnung_Positionen AP
    JOIN Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
    WHERE AR.Zahlungsdatum IS NULL OR AR.ZahlungsbetragBrutto IS NULL
),
Placeholder_Media AS (
    SELECT SUM(Nettobetrag) AS sum_placeholder_media_revenue
    FROM Abrechnung_Positionen
    WHERE Bildnummer = 100000000
),
Faulty_Invoices AS (
    SELECT COUNT(*) AS count_invoices_missing_positions
    FROM Abrechnung_Rechnungen AR
    LEFT JOIN Abrechnung_Positionen AP ON AP.ReId = AR.ReNummer
    WHERE AP.id IS NULL
)
SELECT *
FROM Faulty_Positions, Placeholder_Media, Faulty_Invoices;
GO