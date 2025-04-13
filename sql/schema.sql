-- First create a new database for the IMAGO project
CREATE DATABASE ImagoTest;
GO

-- Switch to the newly created database
USE ImagoTest;
GO

-- Table: Abrechnung_Positionen (Invoice Line Items)
-- Purpose: Stores individual line items for each invoice
CREATE TABLE [Abrechnung_Positionen] (
    [id] [int] IDENTITY(1,1) NOT NULL,
    [ReId] [int] NULL,
    [KdNr] [decimal](5, 0) NULL,
    [Nettobetrag] [money] NULL,
    [Bildnummer] [int] NULL,
    [VerDatum] [datetime] NULL,
    CONSTRAINT [PK_Abrechnung_Positionen] PRIMARY KEY CLUSTERED (
        [id] ASC
    ) WITH (
        PAD_INDEX = OFF,
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON,
        FILLFACTOR = 99
    ) ON [PRIMARY]
) ON [PRIMARY];
GO

-- Table: Abrechnung_Rechnungen (Invoices)
-- Purpose: Stores invoice header information
CREATE TABLE [Abrechnung_Rechnungen] (
    [ReNummer] [int] IDENTITY(1,1) NOT NULL,
    [SummeNetto] [money] NULL,
    [MwStSatz] [int] NOT NULL,
    [ZahlungsbetragBrutto] [money] NULL,
    [KdNr] [decimal](5, 0) NULL,
    [Summenebenkosten] [money] NULL,
    [ReDatum] [datetime] NOT NULL,
    [Zahlungsdatum] [datetime] NULL,
    CONSTRAINT [PK_Abrechnung_Rechnungen] PRIMARY KEY CLUSTERED (
        [ReNummer] ASC
    ) WITH (
        PAD_INDEX = OFF,
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON,
        FILLFACTOR = 99
    ) ON [PRIMARY]
) ON [PRIMARY];
GO

-- Table: Abrechnung_Kunden (Customers)
-- Purpose: Stores customer/publisher information
CREATE TABLE [Abrechnung_Kunden] (
    [id] [int] IDENTITY(1,1) NOT NULL,
    [Kdnr] [decimal](5, 0) NULL,
    [Verlagsname] [varchar](100) NULL,
    [Region] [varchar](50) NULL,
    CONSTRAINT [PK_Abrechnung_Kunden] PRIMARY KEY CLUSTERED (
        [id] ASC
    ) WITH (
        PAD_INDEX = OFF,
        STATISTICS_NORECOMPUTE = OFF,
        IGNORE_DUP_KEY = OFF,  
        ALLOW_ROW_LOCKS = ON,
        ALLOW_PAGE_LOCKS = ON,
        FILLFACTOR = 99
    ) ON [PRIMARY]
) ON [PRIMARY];
GO