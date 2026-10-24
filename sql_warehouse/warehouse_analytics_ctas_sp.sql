-- ==============================================================================
-- FABRIC DATA WAREHOUSE: DATENBANKÜBERGREIFENDE ANALYSEN & STORED PROCEDURES
-- Arbeitsbereich: DP700_Supermarkt_Practice (FrischMarkt GmbH)
-- Warehouse: wh_analytics
-- Quelle: OneLake-Lakehouse-Shortcut / datenbankübergreifende Abfrage auf lh_silver
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. CTAS: Kategorie-Umsatz-Datamart (Vorberechnete Aggregationstabelle)
-- ------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Kategorie_Umsatz_Summary', 'U') IS NOT NULL
    DROP TABLE dbo.Kategorie_Umsatz_Summary;
GO

CREATE TABLE dbo.Kategorie_Umsatz_Summary AS
SELECT 
    p.KategorieName,
    f.Jahr,
    COUNT(DISTINCT f.EinkaufID) AS AnzahlEinkaeufe,
    SUM(f.Menge) AS Gesamtmenge,
    SUM(f.Gesamtbetrag) AS Gesamtumsatz,
    ROUND(AVG(f.Gesamtbetrag), 2) AS DurchschnittlicherUmsatzJePosten
FROM [lh_silver].[dbo].[fct_verkaeufe] f
LEFT JOIN [lh_silver].[dbo].[dim_produkte] p 
    ON f.ProduktID = p.ProduktID
GROUP BY p.KategorieName, f.Jahr;
GO

-- ------------------------------------------------------------------------------
-- 2. CTAS: Filial-Performance-Datamart (Standortvergleich inkl. Braunschweig)
-- ------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Filiale_Performance_Summary', 'U') IS NOT NULL
    DROP TABLE dbo.Filiale_Performance_Summary;
GO

CREATE TABLE dbo.Filiale_Performance_Summary AS
SELECT 
    fil.FilialName,
    fil.Stadt,
    f.Jahr,
    COUNT(DISTINCT f.EinkaufID) AS AnzahlBons,
    COUNT(f.PositionID) AS GesamtePositionen,
    SUM(f.Menge) AS AbsatzMenge,
    SUM(f.Gesamtbetrag) AS Bruttoumsatz,
    ROUND(SUM(f.Gesamtbetrag) / NULLIF(COUNT(DISTINCT f.EinkaufID), 0), 2) AS Durchschnittsbon_EUR
FROM [lh_silver].[dbo].[fct_verkaeufe] f
LEFT JOIN [lh_silver].[dbo].[dim_filialen] fil 
    ON f.FilialeID = fil.FilialeID
GROUP BY fil.FilialName, fil.Stadt, f.Jahr;
GO

-- ------------------------------------------------------------------------------
-- 3. STORED PROCEDURES: Idempotente Aktualisierung der Datamarts
-- ------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_Refresh_Analytics_Datamarts
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 3.1. Kategorie Summary aktualisieren
    TRUNCATE TABLE dbo.Kategorie_Umsatz_Summary;
    
    INSERT INTO dbo.Kategorie_Umsatz_Summary (
        KategorieName,
        Jahr,
        AnzahlEinkaeufe,
        Gesamtmenge,
        Gesamtumsatz,
        DurchschnittlicherUmsatzJePosten
    )
    SELECT 
        p.KategorieName,
        f.Jahr,
        COUNT(DISTINCT f.EinkaufID),
        SUM(f.Menge),
        SUM(f.Gesamtbetrag),
        ROUND(AVG(f.Gesamtbetrag), 2)
    FROM [lh_silver].[dbo].[fct_verkaeufe] f
    LEFT JOIN [lh_silver].[dbo].[dim_produkte] p ON f.ProduktID = p.ProduktID
    GROUP BY p.KategorieName, f.Jahr;

    -- 3.2. Filial Performance aktualisieren
    TRUNCATE TABLE dbo.Filiale_Performance_Summary;

    INSERT INTO dbo.Filiale_Performance_Summary (
        FilialName,
        Stadt,
        Jahr,
        AnzahlBons,
        GesamtePositionen,
        AbsatzMenge,
        Bruttoumsatz,
        Durchschnittsbon_EUR
    )
    SELECT 
        fil.FilialName,
        fil.Stadt,
        f.Jahr,
        COUNT(DISTINCT f.EinkaufID),
        COUNT(f.PositionID),
        SUM(f.Menge),
        SUM(f.Gesamtbetrag),
        ROUND(SUM(f.Gesamtbetrag) / NULLIF(COUNT(DISTINCT f.EinkaufID), 0), 2)
    FROM [lh_silver].[dbo].[fct_verkaeufe] f
    LEFT JOIN [lh_silver].[dbo].[dim_filialen] fil ON f.FilialeID = fil.FilialeID
    GROUP BY fil.FilialName, fil.Stadt, f.Jahr;
END;
GO

-- ------------------------------------------------------------------------------
-- 4. TESTLAUF DER PROZEDUR & ERGEBNISPRÜFUNG
-- ------------------------------------------------------------------------------
EXEC dbo.usp_Refresh_Analytics_Datamarts;
GO

SELECT * FROM dbo.Kategorie_Umsatz_Summary ORDER BY Gesamtumsatz DESC;
SELECT * FROM dbo.Filiale_Performance_Summary ORDER BY Bruttoumsatz DESC;
GO
