# %% [markdown]
# # PySpark ETL: Bronze to Silver (Star Schema Transformation)
# **Workspace:** DP700_Supermarkt_Practice
# **Projekt:** Supermarkt Analytics & BI-Plattform
# **Ziel:** Bereinigung der Bronze-Rohdaten und Überführung in ein optimiertes Sternschema (Delta Lake)

# %%
from pyspark.sql.functions import col, to_date, year, month, date_format, trim, concat_ws, round, coalesce, lit

# ---------------------------------------------------------
# 1. BRONZE TABELLEN LADEN (OneLake Delta / Raw)
# ---------------------------------------------------------
print("Lade Rohdaten aus lh_bronze...")

df_raw_einkaeufe   = spark.read.table("lh_bronze.dbo.einkaeufe")
df_raw_positionen  = spark.read.table("lh_bronze.dbo.einkauf_positionen")
df_raw_kunden      = spark.read.table("lh_bronze.dbo.kunden")
df_raw_mitarbeiter = spark.read.table("lh_bronze.dbo.mitarbeiter")
df_raw_produkte    = spark.read.table("lh_bronze.dbo.produkte")
df_raw_kategorien  = spark.read.table("lh_bronze.dbo.kategorien")

# %% [markdown]
# ## 2. Dimensionstabellen erstellen (Cleansing & Dedup)

# %%
# Dim Kunden: Bereinigung, String-Trim und Namens-Zusammenführung
df_dim_kunden = df_raw_kunden \
    .filter(col("KundeID").isNotNull()) \
    .withColumn("KundeID", col("KundeID").cast("int")) \
    .withColumn("Vorname", trim(col("Vorname"))) \
    .withColumn("Nachname", trim(col("Nachname"))) \
    .withColumn("VollstaendigerName", concat_ws(" ", col("Vorname"), col("Nachname"))) \
    .withColumn("Stadt", trim(col("Stadt"))) \
    .withColumn("Geburtsdatum", to_date(col("Geburtsdatum"))) \
    .select("KundeID", "Vorname", "Nachname", "VollstaendigerName", "Stadt", "Geburtsdatum") \
    .dropDuplicates(["KundeID"])

# Dim Produkte: Denormalisierung (Produkte + KategorieName für Star Schema)
df_dim_produkte = df_raw_produkte.alias("p") \
    .join(df_raw_kategorien.alias("k"), col("p.KategorieID") == col("k.KategorieID"), "left") \
    .select(
        col("p.ProduktID").cast("int").alias("ProduktID"),
        trim(col("p.ProduktName")).alias("ProduktName"),
        col("p.KategorieID").cast("int").alias("KategorieID"),
        coalesce(trim(col("k.KategorieName")), lit("Unbekannt")).alias("KategorieName"),
        col("p.Preis").cast("decimal(10,2)").alias("Listenpreis"),
        col("p.Lagerbestand").cast("int").alias("Lagerbestand")
    ) \
    .dropDuplicates(["ProduktID"])

# Dim Mitarbeiter: Bereinigung & Typkonvertierung
df_dim_mitarbeiter = df_raw_mitarbeiter \
    .filter(col("MitarbeiterID").isNotNull()) \
    .withColumn("MitarbeiterID", col("MitarbeiterID").cast("int")) \
    .withColumn("MitarbeiterName", concat_ws(" ", trim(col("Vorname")), trim(col("Nachname")))) \
    .withColumn("Position", trim(col("Position"))) \
    .withColumn("Einstellungsdatum", to_date(col("Einstellungsdatum"))) \
    .select("MitarbeiterID", "MitarbeiterName", "Position", "Einstellungsdatum") \
    .dropDuplicates(["MitarbeiterID"])

# %% [markdown]
# ## 3. Faktentabelle erstellen (Einkaufskopf + Positionen)

# %%
df_fct_verkaeufe = df_raw_positionen \
    .join(df_raw_einkaeufe, "EinkaufID", "inner") \
    .withColumn("EinkaufDatum", to_date(col("EinkaufDatum"))) \
    .withColumn("Jahr", year(col("EinkaufDatum"))) \
    .withColumn("Monat", month(col("EinkaufDatum"))) \
    .withColumn("Wochentag", date_format(col("EinkaufDatum"), "EEEE")) \
    .withColumn("Menge", col("Menge").cast("int")) \
    .withColumn("Einzelpreis", col("Einzelpreis").cast("decimal(10,2)")) \
    .withColumn("Gesamtbetrag", round(col("Menge") * col("Einzelpreis"), 2)) \
    .select(
        col("PositionID").cast("int"),
        col("EinkaufID").cast("int"),
        col("KundeID").cast("int"),
        col("MitarbeiterID").cast("int"),
        col("ProduktID").cast("int"),
        col("EinkaufDatum"),
        col("Jahr"),
        col("Monat"),
        col("Wochentag"),
        col("Menge"),
        col("Einzelpreis"),
        col("Gesamtbetrag")
    )

# %% [markdown]
# ## 4. In Silver als Delta Tabellen persistieren

# %%
df_dim_kunden.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.dim_kunden")
df_dim_produkte.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.dim_produkte")
df_dim_mitarbeiter.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.dim_mitarbeiter")
df_fct_verkaeufe.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.fct_verkaeufe")

print("Silver-Tabellen (Star Schema) erfolgreich als Delta Parquet geschrieben!")

# %% [markdown]
# ## 5. Delta Lake Wartung: Z-Order & History

# %%
# %%sql
# -- Z-Order auf häufig gefilterte und gejointen Spalten anwenden
# OPTIMIZE lh_silver.dbo.fct_verkaeufe ZORDER BY (EinkaufDatum, ProduktID);

# -- Versionskontrolle & Delta Transaktionslog prüfen
# DESCRIBE HISTORY lh_silver.dbo.fct_verkaeufe;
