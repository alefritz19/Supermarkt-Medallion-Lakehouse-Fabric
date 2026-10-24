# 🛒 Supermarkt-Medallion-Datenplattform & Power BI Direct Lake
### End-to-End Lakehouse auf Microsoft Fabric (Portfolio-Projekt zur Vorbereitung auf DP-700 & PL-300)

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-OneLake-0078D4?logo=microsoft)](https://learn.microsoft.com/fabric/)
[![Apache Spark](https://img.shields.io/badge/PySpark-Delta_Lake-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![T-SQL](https://img.shields.io/badge/T--SQL-Fabric_Warehouse-CC292B?logo=microsoftsqlserver)](https://learn.microsoft.com/fabric/data-warehouse/)
[![Power BI](https://img.shields.io/badge/Power_BI-Direct_Lake-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📌 Übersicht & Geschäftskontext

Dieses Portfolio-Projekt dokumentiert eine End-to-End **Cloud-Datenplattform** für das fiktive deutsche Lebensmitteleinzelhandels-Unternehmen **FrischMarkt GmbH** (5 Filialen, 8 Warengruppen, 120 SKUs, ~755.000 Kassenbeleg-Positionen, synthetische Daten).

Ziel der Plattform ist die Überwindung traditioneller ETL- und Import-Grenzen: Operative Kassendaten werden über eine dreistufige **Medallion-Architektur (Bronze ➔ Silver ➔ Gold)** in **Microsoft Fabric** überführt, mit **PySpark** und **Delta Lake** qualitätsgesichert und über **Power BI Direct Lake** ohne Duplikation oder Datenaktualisierungs-Latenz für das Management bereitgestellt.

---

## 🏗️ End-to-End Architektur

```mermaid
flowchart TD
    subgraph OnPrem["1. Operative Datenquellen"]
        SQL["Microsoft SQL Server / POS ERP (SupermarktDWH)\nTransaktionale Tabellen:\n- einkaeufe, einkauf_positionen\n- filialen, kunden, mitarbeiter, produkte, kategorien"]
    end

    subgraph FabricBronze["2. Ingestion & Bronze Layer (Raw)"]
        GW["Fabric Data Factory / Copy Activity\nDatenübernahme aus SQL Server"]
        Bronze["Lakehouse: lh_bronze\nFormat: Delta Parquet (Rohtabellen)\n- Unveränderte Rohdaten\n- Vollständig neu ladbar & prüfbar"]
    end

    subgraph FabricSilver["3. Transformation & Silver Layer (bereinigt)"]
        NB["PySpark Notebook\nnb_bronze_to_silver_supermarkt\n- Schema-Validierung & Typkonvertierung\n- String-Bereinigung, Deduplizierung & Coalesce\n- Kimball-Sternschema (dim_filialen, dim_produkte, dim_datum...)\n- V-Order- & Z-ORDER-Optimierung"]
        Silver["Lakehouse: lh_silver (Delta Lake)\n- dim_filialen (5 Standorte)\n- dim_produkte (120 SKUs, denormalisiert)\n- dim_kunden (3.000 Treuekunden)\n- dim_mitarbeiter (40 Mitarbeiter)\n- dim_datum (Vollständiger Kalender)\n- fct_verkaeufe (~755.000 Positionen)"]
    end

    subgraph FabricGold["4. Bereitstellung & Gold Layer (fachlich aufbereitet)"]
        Gold["Lakehouse: lh_gold\nZero-Copy OneLake Shortcuts\nVerknüpfung auf lh_silver.dbo\nSpeicherüberhang: 0 MB!"]
        WH["Fabric Data Warehouse: wh_analytics\nDatenbankübergreifende T-SQL-Datamarts\n- CTAS: dbo.Kategorie_Umsatz_Summary\n- CTAS: dbo.Filiale_Performance_Summary\n- Stored Proc: dbo.usp_Refresh_Analytics_Datamarts"]
    end

    subgraph PowerBI["5. Business Intelligence & Direct Lake"]
        SM["Semantisches Modell (Direct Lake): sm_supermarkt_sales\n- Liest Delta Parquet direkt aus dem OneLake-Speicher\n- Keine ETL-Latenz, keine Import-Größenlimits"]
        Report["Power-BI-Management-Dashboard\n- KPI-Karten (Umsatz, Bons, Durchschnittsbon)\n- Filialvergleich & Vormonats-/Vorjahresanalysen\n- Umsatzanteile je Kategorie"]
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
│   └── nb_bronze_to_silver_supermarkt.py   # PySpark-ETL: Bereinigung, Sternschema, Kalender & Delta Z-Order
├── sql_warehouse/
│   └── warehouse_analytics_ctas_sp.sql     # Fabric Data Warehouse: CTAS & Stored Procedures (datenbankübergreifend)
├── power_bi/
│   └── dax_measures_supermarkt.dax         # Explizite DAX-Measures (Zeitintelligenz, Vorjahresvergleich, Filial-KPIs)
├── .gitattributes                          # Linguist-Sprachzuordnung (TSQL, DAX, Python)
├── .gitignore
├── LICENSE                                 # MIT-Lizenz
└── README.md                               # Vollständige Dokumentation & Runbook
```

---

## 🛠️ Technische Kernkomponenten im Detail

### 1. PySpark & Delta Lake (Bronze ➔ Silver)
*Datei:* [`notebooks/nb_bronze_to_silver_supermarkt.py`](notebooks/nb_bronze_to_silver_supermarkt.py)
- **Denormalisierung & Sternschema:** Überführung flacher operativer Tabellen in ein analytisches Sternschema (`dim_filialen`, `dim_produkte`, `dim_kunden`, `dim_mitarbeiter`, `dim_datum`, `fct_verkaeufe`).
- **Datenhygiene:** Standardisierung von Zeichenketten (`trim`), Nullwert-Absicherung (`coalesce`) und deterministische Deduplizierung über Primärschlüssel.
- **Kalender-Generierung:** Automatische Erzeugung einer lückenlosen Datumstabelle (2024–2026) mit deutschen Wochentags- und Monatsnamen in PySpark für DAX-Zeitintelligenzfunktionen in Direct Lake.
- **Performance-Optimierung:** Anwendung von `OPTIMIZE` mit multidimensionalem `Z-ORDER BY (EinkaufDatum, FilialeID, ProduktID)` für beschleunigtes File-Skipping bei analytischen Abfragen.

### 2. Fabric Data Warehouse (datenbankübergreifendes T-SQL)
*Datei:* [`sql_warehouse/warehouse_analytics_ctas_sp.sql`](sql_warehouse/warehouse_analytics_ctas_sp.sql)
- **Zero-Copy Shortcuts:** Nahtloser SQL-Zugriff aus dem Warehouse `wh_analytics` auf den Lakehouse Delta Layer (`[lh_silver].[dbo].[fct_verkaeufe]`) ohne Datenverdopplung.
- **CTAS (CREATE TABLE AS SELECT):** Vorberechnete Aggregations-Tabellen für Vorstandspräsentationen und Fachbereichsanalysen (`dbo.Kategorie_Umsatz_Summary`, `dbo.Filiale_Performance_Summary`).
- **Idempotente Stored Procedures:** Automatisierte Aktualisierungsroutinen via `dbo.usp_Refresh_Analytics_Datamarts` für Pipeline-Orchestrierungen.

### 3. Power BI Direct Lake & DAX Engine
*Datei:* [`power_bi/dax_measures_supermarkt.dax`](power_bi/dax_measures_supermarkt.dax)
- **Direct-Lake-Modus:** Direkte Abfrage der Delta Parquet Dateien in OneLake ohne herkömmlichen Import und ohne DirectQuery-Performanceverluste.
- **Explizite Kennzahlen:**
  - `Total_Umsatz = SUM(fct_verkaeufe[Gesamtbetrag])`
  - `Durchschnittsbon = DIVIDE([Total_Umsatz], [Anzahl_Transaktionen], 0)`
  - `Total_Umsatz_Vorjahr = CALCULATE([Total_Umsatz], SAMEPERIODLASTYEAR('dim_datum'[Datum]))`
  - `Umsatz_YoY_Wachstum_Prozent = DIVIDE([Umsatz_YoY_Wachstum_Absolut], [Total_Umsatz_Vorjahr], 0)`

---

## 🧠 Architekturentscheidungen (DP-700- & PL-300-Leitfaden)

> [!NOTE]
> **Warum Direct Lake statt DirectQuery oder Import?**  
> Direct Lake lädt Parquet-Spaltendaten direkt aus OneLake in den VertiPaq-Hauptspeicher der Power-BI-Engine. Dadurch entfallen sowohl zeitaufwendige geplante Aktualisierungen (Import) als auch die langsame Übersetzung in SQL-Abfragen (DirectQuery).

> [!IMPORTANT]
> **Fallback vermeiden (Direct Lake ➔ DirectQuery):**  
> 1. **V-Order aktiv halten:** Gewährleistet native VertiPaq-kompatible Kompression in der Parquet-Datei.
> 2. **Keine berechneten Spalten in DAX:** Alle Transformationen und Spalten werden upstream in PySpark oder SQL ausgeführt.
> 3. **Kapazitätslimits überwachen:** Überschreitet die Modellgröße die Paging-Grenzen der Fabric-SKU, fällt Power BI automatisch auf DirectQuery zurück. Durch schlanke Datentypen (`INT`, `DECIMAL(10,2)`) und Z-Ordering bleibt das Modell dauerhaft im Direct-Lake-Modus.

---

## 🚀 Schritt-für-Schritt-Anleitung (Reproduktion)

1. **Arbeitsbereich anlegen:** Microsoft-Fabric-Arbeitsbereich mit Fabric- oder Testkapazität erstellen (`DP700_Supermarkt_Practice`).
2. **Lakehouses initialisieren:**
   - `lh_bronze`: Speicherort für unveränderte Rohdaten.
   - `lh_silver`: Bereinigter Delta Lake Layer mit Star Schema.
   - `lh_gold`: Gold Layer (Zero-Copy OneLake Shortcuts auf `lh_silver`).
3. **Warehouse anlegen:** `wh_analytics` für T-SQL-Abfragen und datenbankübergreifendes CTAS.
4. **Notebook ausführen:** `notebooks/nb_bronze_to_silver_supermarkt.py` im Fabric-Arbeitsbereich importieren und ausführen.
5. **Warehouse-Skript bereitstellen:** `sql_warehouse/warehouse_analytics_ctas_sp.sql` ausführen und Stored Procedure testen.
6. **Semantisches Modell konfigurieren:** Neues Direct-Lake-Modell auf `lh_gold` erstellen und DAX-Measures hinterlegen.
7. **Dashboard bereitstellen:** Power-BI-Management-Dashboard mit KPI-Karten, Treemaps und Monatsvergleichen veröffentlichen.

---

## 👤 Entwickler & Kontakt

**Alexander Fritzler**  
*Junior Data Engineer (Microsoft Fabric · Snowflake · SQL Server · PySpark)*  
- 💼 LinkedIn: [Alexander Fritzler | Profil anzeigen](https://www.linkedin.com/in/alexander-fritzler-214628356/)  
- 🐙 GitHub: [@alefritz19](https://github.com/alefritz19)  
- 📧 Kontakt: alefritz19@gmail.com
