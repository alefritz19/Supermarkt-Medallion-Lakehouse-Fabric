# 🛒 Supermarkt Medallion Data Platform & Power BI Direct Lake
### End-to-End Enterprise Lakehouse on Microsoft Fabric (DP-700 & PL-300)

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-OneLake-0078D4?logo=microsoft)](https://learn.microsoft.com/fabric/)
[![Apache Spark](https://img.shields.io/badge/PySpark-Delta_Lake-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![T-SQL](https://img.shields.io/badge/T--SQL-Synapse_Warehouse-CC292B?logo=microsoftsqlserver)](https://learn.microsoft.com/sql/t-sql/)
[![Power BI](https://img.shields.io/badge/Power_BI-Direct_Lake-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📌 Übersicht & Geschäftskontext

Dieses Repository beinhaltet die vollständige, produktionsreife Implementierung einer modernen **Cloud Data Platform** für den Einzelhandel (Supermarkt-Filialen & E-Commerce).

Ziel ist es, transaktionale Kassensysteme und ERP-Daten über eine dreistufige **Medallion-Architektur (Bronze ➔ Silver ➔ Gold)** in **Microsoft Fabric** zu überführen, mittels **PySpark** und **Delta Lake** zu bereinigen und über **Power BI Direct Lake** ohne Duplikation oder ETL-Latenz für das Management bereitzustellen.

---

## 🏗️ End-to-End Architektur

```mermaid
flowchart TD
    subgraph OnPrem["1. Operative Datenquellen"]
        SQL["Microsoft SQL Server (SQLEXPRESS)\nTransaktionale Tabellen:\n- einkaeufe, einkauf_positionen\n- kunden, mitarbeiter, produkte, kategorien"]
    end

    subgraph FabricBronze["2. Ingestion & Bronze Layer (Raw)"]
        GW["On-Premises Data Gateway / Data Factory\nCopy Activity Pipeline (cj_ingest_sql)"]
        Bronze["Lakehouse: lh_bronze\nFormat: Delta Parquet (Raw Tabellen)\n- Unveränderte Rohdaten / Re-Ingest fähig"]
    end

    subgraph FabricSilver["3. Transformation & Silver Layer (Curated)"]
        NB["PySpark Notebook\nnb_bronze_to_silver_supermarkt\n- Schema-Validierung & Typkonvertierung\n- String Cleansing, Dedup & Coalesce\n- Sternschema-Modellierung (Star Schema)"]
        Silver["Lakehouse: lh_silver (Delta Lake)\n- dim_kunden\n- dim_produkte (Denormalisiert)\n- dim_mitarbeiter\n- fct_verkaeufe\nOPTIMIZE & Z-ORDER (EinkaufDatum, ProduktID)"]
    end

    subgraph FabricGold["4. Serving & Gold Layer (Business Ready)"]
        Gold["Lakehouse: lh_gold\nZero-Copy OneLake Schema Shortcuts\nVerknüpfung auf lh_silver.dbo\nSpeicherverbrauch: 0 MB!"]
        WH["Fabric Data Warehouse: wh_analytics\nCross-Database T-SQL Analytics\n- CTAS: dbo.Kategorie_Umsatz_Summary\n- Stored Proc: dbo.usp_Refresh_Kategorie_Summary"]
    end

    subgraph PowerBI["5. Business Intelligence & Direct Lake"]
        SM["Direct Lake Semantikmodell: sm_supermarkt_sales\n- Direkter Zugriff auf Delta Parquet\n- Keine Import-Latenz, keine Aktualisierungsdauer"]
        Report["Power BI Management Dashboard\n- KPI Scorecards (Umsatz, Menge, Bons)\n- Matrix mit Kategorie-Detail\n- Time Intelligence & YoY Wachstumsanalysen"]
    end

    SQL --> GW --> Bronze
    Bronze --> NB --> Silver
    Silver --> Gold
    Silver --> WH
    Gold --> SM
    WH --> SM
    SM --> Report
```

---

## 📂 Repository-Struktur

```text
├── notebooks/
│   └── nb_bronze_to_silver_supermarkt.py   # PySpark ETL: Cleansing, Star Schema & Delta Z-Order
├── sql_warehouse/
│   └── warehouse_analytics_ctas_sp.sql     # Fabric Data Warehouse CTAS & Stored Procedures
├── power_bi/
│   └── dax_measures_supermarkt.dax         # Explizite DAX Measures (Time Intelligence, YoY, KPIs)
├── .gitignore
└── README.md                               # Vollständige Dokumentation & Runbook
```

---

## 🛠️ Technische Kernkomponenten

### 1. PySpark & Delta Lake (Bronze ➔ Silver)
*Datei:* [`notebooks/nb_bronze_to_silver_supermarkt.py`](notebooks/nb_bronze_to_silver_supermarkt.py)
- **Denormalisierung:** Automatisches Auflösen von hierarchischen Kategorien in `dim_produkte` für das Sternschema.
- **Datenhygiene:** Bereinigung von Leerzeichen (`trim`), Nullwert-Absicherung (`coalesce`) und deterministische Deduplizierung über Primärschlüssel.
- **Faktentabellen-Generierung:** Verknüpfung von Belegkopf und Positionen, Berechnung von Umsatzkennzahlen und automatisches Extrahieren von Datumsdimensionen (`Jahr`, `Monat`, `Wochentag`).
- **Performance-Tuning:** Anwendung von `OPTIMIZE` mit multidimensionalem `Z-ORDER BY (EinkaufDatum, ProduktID)` für beschleunigtes File-Skipping bei analytischen Abfragen.

### 2. Fabric Synapse Data Warehouse (T-SQL CTAS)
*Datei:* [`sql_warehouse/warehouse_analytics_ctas_sp.sql`](sql_warehouse/warehouse_analytics_ctas_sp.sql)
- **Cross-Database Abfragen:** Nahtlose SQL-Abfragen über den Lakehouse-Shortcut (`[lh_silver].[dbo].[fct_verkaeufe]`).
- **CTAS (CREATE TABLE AS SELECT):** Performante Vorberechnung aggregierter Datamarts für schnelle Executive-Reports.
- **Idempotente Stored Procedures:** Automatisiertes Leeren (`TRUNCATE`) und Wiederbefüllen via `dbo.usp_Refresh_Kategorie_Summary`.

### 3. Power BI Direct Lake & DAX
*Datei:* [`power_bi/dax_measures_supermarkt.dax`](power_bi/dax_measures_supermarkt.dax)
- **Direct Lake Modus:** Direkte Abfrage der Delta Parquet Dateien in OneLake ohne herkömmlichen Datenimport und ohne DirectQuery-Performanceverluste.
- **Explizite DAX Measures:**
  - `Total_Umsatz = SUM(fct_verkaeufe[Gesamtbetrag])`
  - `Durchschnittsbon = DIVIDE([Total_Umsatz], [Anzahl_Transaktionen], 0)`
  - `Total_Umsatz_Vorjahr = CALCULATE([Total_Umsatz], SAMEPERIODLASTYEAR('dim_datum'[Datum]))`
  - `Umsatz_YoY_Wachstum_% = DIVIDE([Umsatz_YoY_Wachstum_Absolut], [Total_Umsatz_Vorjahr], 0)`

---

## 🚀 Schritt-für-Schritt Runbook (Reproduktion)

1. **Workspace anlegen:** Microsoft Fabric Workspace mit Fabric- oder Trial-Kapazität erstellen (`DP700_Supermarkt_Practice`).
2. **Lakehouses initialisieren:**
   - `lh_bronze`: Speicherort für unveränderte Rohdaten.
   - `lh_silver`: Bereinigter Delta Lake Layer.
   - `lh_gold`: Business-Layer (Zero-Copy Shortcuts auf `lh_silver`).
3. **Warehouse anlegen:** `wh_analytics` für T-SQL Serving und Cross-DB CTAS.
4. **Notebook ausführen:** `notebooks/nb_bronze_to_silver_supermarkt.py` im Fabric-Arbeitsbereich importieren und ausführen.
5. **Warehouse Skript deployen:** `sql_warehouse/warehouse_analytics_ctas_sp.sql` ausführen und Stored Procedure testen.
6. **Semantikmodell konfigurieren:** Neues Direct Lake Modell auf `lh_gold` erstellen und DAX-Measures hinterlegen.
7. **Dashboard bereitstellen:** Power BI Management Dashboard mit KPI Scorecards, Treemaps und Monatsvergleichen publizieren.

---

## 👤 Entwickler

**Alexander Fritzler**  
*Data Analyst & Microsoft Fabric Data Engineer*  
- 💼 LinkedIn: [Alexander Fritzler | Profil anzeigen](https://www.linkedin.com/in/alexander-fritzler-214628356/)  
- 🐙 GitHub: [@alefritz19](https://github.com/alefritz19)  
- 📧 Kontakt: alefritz19@gmail.com
