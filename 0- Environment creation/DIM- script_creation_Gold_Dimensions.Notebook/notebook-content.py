# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "05a7438c-de65-493f-9ab4-a14c2617a07b",
# META       "default_lakehouse_name": "lkh_gold",
# META       "default_lakehouse_workspace_id": "d500bac7-7518-4386-b1f8-857ca400e400",
# META       "known_lakehouses": [
# META         {
# META           "id": "05a7438c-de65-493f-9ab4-a14c2617a07b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ===========================================================
# MAGIC    FABRIC LAKEHOUSE - GOLD LAYER
# MAGIC    Création des tables de dimension communes aux 3 tables BRC
# MAGIC    Schéma : dim
# MAGIC    =========================================================== */
# MAGIC 
# MAGIC /* ---------- Création du schéma dim ---------- */
# MAGIC CREATE SCHEMA IF NOT EXISTS dim;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Dim_Semaine : dimension temporelle hebdomadaire
# MAGIC    Clé : Numero_Semaine (format YYYYWW, ex : 202545)
# MAGIC    Alimentée à partir du champ Numero_Semaine des 3 tables
# MAGIC    ========================================================== */
# MAGIC DROP TABLE IF EXISTS dim.Dim_Semaine;
# MAGIC 
# MAGIC CREATE TABLE dim.Dim_Semaine (
# MAGIC     Numero_Semaine          INT             NOT NULL,
# MAGIC     Annee                   INT,
# MAGIC     Semaine                 INT,
# MAGIC     Date_Debut_Semaine      DATE,
# MAGIC     Date_Fin_Semaine        DATE,
# MAGIC     Libelle_Semaine         VARCHAR(20)
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Dim_Date : calendrier standard (grain journalier)
# MAGIC    Clé : Date_Key (DATE)
# MAGIC    Liée à Date_limite_de_traitement, Timestamp_creation, etc.
# MAGIC    ========================================================== */
# MAGIC DROP TABLE IF EXISTS dim.Dim_Date;
# MAGIC 
# MAGIC CREATE TABLE dim.Dim_Date (
# MAGIC     Date_Key                DATE            NOT NULL,
# MAGIC     Annee                   INT,
# MAGIC     Trimestre               INT,
# MAGIC     Mois                    INT,
# MAGIC     Libelle_Mois            VARCHAR(20),
# MAGIC     Semaine_ISO             INT,
# MAGIC     Jour_Du_Mois            INT,
# MAGIC     Jour_Semaine            INT,
# MAGIC     Libelle_Jour            VARCHAR(10),
# MAGIC     Est_Jour_Ouvre          INT
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Dim_Code_GPS : dimension site / organisme de gestion
# MAGIC    Clé : Code_GPS
# MAGIC    Commune aux 3 tables (dp_brc_global, cfiab, dsn)
# MAGIC    ========================================================== */
# MAGIC DROP TABLE IF EXISTS dim.Dim_Code_GPS;
# MAGIC 
# MAGIC CREATE TABLE dim.Dim_Code_GPS (
# MAGIC     Code_GPS                VARCHAR(10)     NOT NULL,
# MAGIC     Libelle_GPS             VARCHAR(100)
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Dim_Type_Tache : dimension type de tâche
# MAGIC    Clé : Code_type_tache
# MAGIC    Commune aux 3 tables
# MAGIC    ========================================================== */
# MAGIC DROP TABLE IF EXISTS dim.Dim_Type_Tache;
# MAGIC 
# MAGIC CREATE TABLE dim.Dim_Type_Tache (
# MAGIC     Code_type_tache                 VARCHAR(11)     NOT NULL,
# MAGIC     Libelle_long_Type_Tache_Metier  VARCHAR(255)
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Dim_IRC : dimension identifiant fonctionnel IRC
# MAGIC    Clé : Identifiant_fonctionnel_IRC_Tache
# MAGIC    Sources : dp2.dp_brc_cfiab, dp3.dp_brc_dsn
# MAGIC    ========================================================== */
# MAGIC DROP TABLE IF EXISTS dim.Dim_IRC;
# MAGIC 
# MAGIC CREATE TABLE dim.Dim_IRC (
# MAGIC     Identifiant_fonctionnel_IRC_Tache       VARCHAR(55)     NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
