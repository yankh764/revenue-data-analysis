-- Switch to the IMAGO database
USE ImagoTest;
GO

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
    AK.Verlagsname AS CustomerName,
    AK.Region AS CustomerRegion
FROM
    Abrechnung_Positionen AP
JOIN
    Abrechnung_Rechnungen AR ON AP.ReId = AR.ReNummer
LEFT JOIN
    Abrechnung_Kunden AK ON AP.KdNr = AK.Kdnr;
GO