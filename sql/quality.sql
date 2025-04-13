-- Switch to the IMAGO database
USE ImagoTest;
GO

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
GO