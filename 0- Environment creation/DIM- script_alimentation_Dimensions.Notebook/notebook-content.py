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
# MAGIC /* ==========================================================
# MAGIC    Alimentation Dim_Semaine
# MAGIC    Extraction de toutes les semaines distinctes des 3 tables
# MAGIC    Numero_Semaine format YYYYWW → on en déduit Année et Semaine
# MAGIC    ========================================================== */
# MAGIC INSERT OVERWRITE dim.Dim_Semaine
# MAGIC 
# MAGIC WITH semaines_distinctes AS (
# MAGIC     SELECT DISTINCT Numero_Semaine FROM dp1.dp_brc_global
# MAGIC     UNION
# MAGIC     SELECT DISTINCT Numero_Semaine FROM dp2.dp_brc_cfiab
# MAGIC     UNION
# MAGIC     SELECT DISTINCT Numero_Semaine FROM dp3.dp_brc_dsn
# MAGIC ),
# MAGIC semaines_enrichies AS (
# MAGIC     SELECT
# MAGIC         Numero_Semaine,
# MAGIC         CAST(FLOOR(Numero_Semaine / 100) AS INT)            AS Annee,
# MAGIC         CAST(Numero_Semaine % 100 AS INT)                   AS Semaine
# MAGIC     FROM semaines_distinctes
# MAGIC     WHERE Numero_Semaine IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     Numero_Semaine,
# MAGIC     Annee,
# MAGIC     Semaine,
# MAGIC     /* Date du lundi de la semaine ISO */
# MAGIC     DATE_ADD(
# MAGIC         MAKE_DATE(Annee, 1, 4),
# MAGIC         (Semaine - 1) * 7 
# MAGIC         - (DAYOFWEEK(MAKE_DATE(Annee, 1, 4)) + 5) % 7
# MAGIC     )                                                       AS Date_Debut_Semaine,
# MAGIC     /* Date du dimanche de la semaine ISO */
# MAGIC     DATE_ADD(
# MAGIC         DATE_ADD(
# MAGIC             MAKE_DATE(Annee, 1, 4),
# MAGIC             (Semaine - 1) * 7 
# MAGIC             - (DAYOFWEEK(MAKE_DATE(Annee, 1, 4)) + 5) % 7
# MAGIC         ),
# MAGIC         6
# MAGIC     )                                                       AS Date_Fin_Semaine,
# MAGIC     CONCAT('S', LPAD(CAST(Semaine AS STRING), 2, '0'), ' - ', CAST(Annee AS STRING))
# MAGIC                                                             AS Libelle_Semaine
# MAGIC FROM semaines_enrichies
# MAGIC ORDER BY Numero_Semaine;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Alimentation Dim_Date
# MAGIC    Génère un calendrier complet couvrant la plage de dates
# MAGIC    trouvées dans les 3 tables (dates de traitement et création)
# MAGIC    ========================================================== */
# MAGIC INSERT OVERWRITE dim.Dim_Date
# MAGIC 
# MAGIC WITH plage_dates AS (
# MAGIC     SELECT 
# MAGIC         DATE '1992-10-04' AS date_min,
# MAGIC         MAX(d) AS date_max
# MAGIC     FROM (
# MAGIC         SELECT MIN(Date_limite_de_traitement) AS d FROM dp1.dp_brc_global WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MAX(Date_limite_de_traitement) FROM dp1.dp_brc_global WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MIN(CAST(Timestamp_creation_tache AS DATE)) FROM dp1.dp_brc_global WHERE Timestamp_creation_tache IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MAX(CAST(Timestamp_creation_tache AS DATE)) FROM dp1.dp_brc_global WHERE Timestamp_creation_tache IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MIN(Date_limite_de_traitement) FROM dp2.dp_brc_cfiab WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MAX(Date_limite_de_traitement) FROM dp2.dp_brc_cfiab WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MIN(Date_limite_de_traitement) FROM dp3.dp_brc_dsn WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC         UNION ALL
# MAGIC         SELECT MAX(Date_limite_de_traitement) FROM dp3.dp_brc_dsn WHERE Date_limite_de_traitement IS NOT NULL
# MAGIC     )
# MAGIC ),
# MAGIC /* Génération de toutes les dates entre min et max */
# MAGIC calendrier AS (
# MAGIC     SELECT 
# MAGIC         DATE_ADD(date_min, pos) AS Date_Key
# MAGIC     FROM plage_dates
# MAGIC     LATERAL VIEW POSEXPLODE(SEQUENCE(0, DATEDIFF(date_max, date_min))) t AS pos, val
# MAGIC )
# MAGIC SELECT
# MAGIC     Date_Key,
# MAGIC     YEAR(Date_Key)                                          AS Annee,
# MAGIC     QUARTER(Date_Key)                                       AS Trimestre,
# MAGIC     MONTH(Date_Key)                                         AS Mois,
# MAGIC     CASE MONTH(Date_Key)
# MAGIC         WHEN 1  THEN 'Janvier'
# MAGIC         WHEN 2  THEN 'Février'
# MAGIC         WHEN 3  THEN 'Mars'
# MAGIC         WHEN 4  THEN 'Avril'
# MAGIC         WHEN 5  THEN 'Mai'
# MAGIC         WHEN 6  THEN 'Juin'
# MAGIC         WHEN 7  THEN 'Juillet'
# MAGIC         WHEN 8  THEN 'Août'
# MAGIC         WHEN 9  THEN 'Septembre'
# MAGIC         WHEN 10 THEN 'Octobre'
# MAGIC         WHEN 11 THEN 'Novembre'
# MAGIC         WHEN 12 THEN 'Décembre'
# MAGIC     END                                                     AS Libelle_Mois,
# MAGIC     WEEKOFYEAR(Date_Key)                                    AS Semaine_ISO,
# MAGIC     DAY(Date_Key)                                           AS Jour_Du_Mois,
# MAGIC     /* 1=Lundi ... 7=Dimanche (ISO) */
# MAGIC     ((DAYOFWEEK(Date_Key) + 5) % 7) + 1                    AS Jour_Semaine,
# MAGIC     CASE ((DAYOFWEEK(Date_Key) + 5) % 7) + 1
# MAGIC         WHEN 1 THEN 'Lundi'
# MAGIC         WHEN 2 THEN 'Mardi'
# MAGIC         WHEN 3 THEN 'Mercredi'
# MAGIC         WHEN 4 THEN 'Jeudi'
# MAGIC         WHEN 5 THEN 'Vendredi'
# MAGIC         WHEN 6 THEN 'Samedi'
# MAGIC         WHEN 7 THEN 'Dimanche'
# MAGIC     END                                                     AS Libelle_Jour,
# MAGIC     /* Est jour ouvré : Lundi(1) à Vendredi(5) */
# MAGIC     CASE WHEN ((DAYOFWEEK(Date_Key) + 5) % 7) + 1 <= 5
# MAGIC          THEN 1 ELSE 0
# MAGIC     END                                                     AS Est_Jour_Ouvre
# MAGIC FROM calendrier
# MAGIC ORDER BY Date_Key;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Alimentation Dim_Code_GPS
# MAGIC    Union des Code_GPS distincts des 3 tables
# MAGIC    ========================================================== */
# MAGIC INSERT OVERWRITE dim.Dim_Code_GPS
# MAGIC 
# MAGIC SELECT
# MAGIC     Code_GPS,
# MAGIC     /* Libellé par défaut = Code_GPS (à enrichir manuellement 
# MAGIC        ou via un référentiel externe si disponible) */
# MAGIC     Code_GPS                                                AS Libelle_GPS
# MAGIC FROM (
# MAGIC     SELECT DISTINCT Code_GPS FROM dp1.dp_brc_global WHERE Code_GPS IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT DISTINCT Code_GPS FROM dp2.dp_brc_cfiab  WHERE Code_GPS IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT DISTINCT Code_GPS FROM dp3.dp_brc_dsn    WHERE Code_GPS IS NOT NULL
# MAGIC )
# MAGIC ORDER BY Code_GPS;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Alimentation Dim_Type_Tache
# MAGIC    Agrège les codes et libellés de type de tâche des 3 tables
# MAGIC    Priorité au libellé le plus long disponible
# MAGIC    ========================================================== */
# MAGIC INSERT OVERWRITE dim.Dim_Type_Tache
# MAGIC 
# MAGIC WITH types_union AS (
# MAGIC     /* dp_brc_global : Code + Libelle_long_Type_Tache_Metier */
# MAGIC     SELECT DISTINCT
# MAGIC         Code_type_tache,
# MAGIC         Libelle_long_Type_Tache_Metier  AS Libelle
# MAGIC     FROM dp1.dp_brc_global
# MAGIC     WHERE Code_type_tache IS NOT NULL
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     /* dp_brc_cfiab : Code + Libelle_long_type_tache */
# MAGIC     SELECT DISTINCT
# MAGIC         Code_type_tache,
# MAGIC         Libelle_long_type_tache         AS Libelle
# MAGIC     FROM dp2.dp_brc_cfiab
# MAGIC     WHERE Code_type_tache IS NOT NULL
# MAGIC 
# MAGIC     UNION
# MAGIC 
# MAGIC     /* dp_brc_dsn : Code + Libelle_long_type_tache */
# MAGIC     SELECT DISTINCT
# MAGIC         Code_type_tache,
# MAGIC         Libelle_long_type_tache         AS Libelle
# MAGIC     FROM dp3.dp_brc_dsn
# MAGIC     WHERE Code_type_tache IS NOT NULL
# MAGIC ),
# MAGIC /* On garde le libellé le plus long par code (le plus informatif) */
# MAGIC types_dedup AS (
# MAGIC     SELECT
# MAGIC         Code_type_tache,
# MAGIC         FIRST_VALUE(Libelle) OVER (
# MAGIC             PARTITION BY Code_type_tache 
# MAGIC             ORDER BY LENGTH(COALESCE(Libelle, '')) DESC
# MAGIC         ) AS Libelle_long_Type_Tache_Metier,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Code_type_tache 
# MAGIC             ORDER BY LENGTH(COALESCE(Libelle, '')) DESC
# MAGIC         ) AS rn
# MAGIC     FROM types_union
# MAGIC )
# MAGIC SELECT
# MAGIC     Code_type_tache,
# MAGIC     Libelle_long_Type_Tache_Metier
# MAGIC FROM types_dedup
# MAGIC WHERE rn = 1
# MAGIC ORDER BY Code_type_tache;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Alimentation Dim_IRC
# MAGIC    Identifiants fonctionnels IRC distincts
# MAGIC    Sources : dp2.dp_brc_cfiab, dp3.dp_brc_dsn
# MAGIC    ========================================================== */
# MAGIC INSERT OVERWRITE dim.Dim_IRC
# MAGIC 
# MAGIC SELECT DISTINCT
# MAGIC     Identifiant_fonctionnel_IRC_Tache
# MAGIC FROM (
# MAGIC     SELECT IRC_primo_affectation_tache AS Identifiant_fonctionnel_IRC_Tache FROM dp1.dp_brc_global WHERE IRC_primo_affectation_tache IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT Identifiant_fonctionnel_IRC_Tache FROM dp2.dp_brc_cfiab WHERE Identifiant_fonctionnel_IRC_Tache IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT Identifiant_fonctionnel_IRC_Tache FROM dp3.dp_brc_dsn   WHERE Identifiant_fonctionnel_IRC_Tache IS NOT NULL
# MAGIC )
# MAGIC ORDER BY Identifiant_fonctionnel_IRC_Tache;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ==========================================================
# MAGIC    Vérification : comptage des lignes par dimension
# MAGIC    ========================================================== */
# MAGIC SELECT 'Dim_Semaine'        AS Dimension, COUNT(*) AS Nb_Lignes FROM dim.Dim_Semaine
# MAGIC UNION ALL
# MAGIC SELECT 'Dim_Date',           COUNT(*) FROM dim.Dim_Date
# MAGIC UNION ALL
# MAGIC SELECT 'Dim_Code_GPS',      COUNT(*) FROM dim.Dim_Code_GPS
# MAGIC UNION ALL
# MAGIC SELECT 'Dim_Type_Tache',    COUNT(*) FROM dim.Dim_Type_Tache
# MAGIC UNION ALL
# MAGIC SELECT 'Dim_IRC',           COUNT(*) FROM dim.Dim_IRC
# MAGIC ORDER BY Dimension;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
