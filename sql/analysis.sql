-- Switch to the IMAGO database
USE ImagoTest;
GO

-- Execute analysis query
WITH Faulty_Positions AS (
    SELECT COUNT(*) AS positions_missing_payment_count
    FROM Abrechnung_Positionen AS AP
    JOIN Abrechnung_Rechnungen AS AR ON AP.ReId = AR.ReNummer
    WHERE AR.Zahlungsdatum IS NULL OR AR.ZahlungsbetragBrutto IS NULL
),
Placeholder_Media AS (
    SELECT SUM(Nettobetrag) AS placeholder_media_revenue_sum
    FROM Abrechnung_Positionen
    WHERE Bildnummer = 100000000
),
Faulty_Invoices AS (
    SELECT COUNT(AR.ReNummer) AS invoices_missing_positions_count
    FROM Abrechnung_Rechnungen AS AR
    LEFT JOIN Abrechnung_Positionen AS AP ON AP.ReId = AR.ReNummer
    WHERE AP.id IS NULL
)
SELECT *
FROM Faulty_Positions, Placeholder_Media, Faulty_Invoices;
GO