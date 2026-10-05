-- ==============================================================================
-- FABRIC DATA WAREHOUSE: CROSS-DATABASE ANALYTICS & STORED PROCEDURE
-- Workspace: DP700_Supermarkt_Practice
-- Warehouse: wh_analytics
-- Quelle: OneLake Lakehouse Shortcut / Cross-DB Query auf lh_silver
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. CTAS (CREATE TABLE AS SELECT): Performante vorberechnete Aggregationstabelle
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
-- 2. STORED PROCEDURE: Automatisierte Aktualisierung der Datamart-Tabelle
-- ------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.usp_Refresh_Kategorie_Summary
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Bestehende Daten leeren (schnell und transaktionssicher)
    TRUNCATE TABLE dbo.Kategorie_Umsatz_Summary;
    
    -- Neue aggregierte Kennzahlen aus dem Silver Delta Layer einfügen
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
        COUNT(DISTINCT f.EinkaufID) AS AnzahlEinkaeufe,
        SUM(f.Menge) AS Gesamtmenge,
        SUM(f.Gesamtbetrag) AS Gesamtumsatz,
        ROUND(AVG(f.Gesamtbetrag), 2) AS DurchschnittlicherUmsatzJePosten
    FROM [lh_silver].[dbo].[fct_verkaeufe] f
    LEFT JOIN [lh_silver].[dbo].[dim_produkte] p 
        ON f.ProduktID = p.ProduktID
    GROUP BY p.KategorieName, f.Jahr;
END;
GO

-- ------------------------------------------------------------------------------
-- 3. TESTLAUF DER PROZEDUR & ERGEBNISPRÜFUNG
-- ------------------------------------------------------------------------------
EXEC dbo.usp_Refresh_Kategorie_Summary;
GO

SELECT 
    KategorieName,
    Jahr,
    AnzahlEinkaeufe,
    Gesamtmenge,
    Gesamtumsatz,
    DurchschnittlicherUmsatzJePosten
FROM dbo.Kategorie_Umsatz_Summary
ORDER BY Gesamtumsatz DESC;
GO
