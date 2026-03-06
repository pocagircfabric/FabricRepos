# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8525f6de-ac22-4dcd-81ca-31d4b406a6aa",
# META       "default_lakehouse_name": "lkh_silver",
# META       "default_lakehouse_workspace_id": "d500bac7-7518-4386-b1f8-857ca400e400",
# META       "known_lakehouses": [
# META         {
# META           "id": "8525f6de-ac22-4dcd-81ca-31d4b406a6aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, trim, to_timestamp, to_date, current_timestamp,
    regexp_extract, concat, lpad, weekofyear, year
)
from pyspark.sql.types import IntegerType, ShortType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 1. Lecture de la table Bronze
# =============================================================================
df_bronze = spark.table("lkh_bronze.dp_scl_tache_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Extraction du numéro de semaine depuis le nom du fichier
#    Exemple filename : ...PSID099.CMN.SCL.ListeDetail.LIQ02-SuiviTachesLiquidation-Stock-HEBDO.20251123.csv?version=...
#    date_fichier = 20251123 (ssaammjj)
#    Numero_Semaine = 202547 (ssaann)
# =============================================================================
df = df_bronze.withColumn(
    "date_fichier",
    regexp_extract(col("File_Name"), r"(\d{8})\.\w+", 1)
).withColumn(
    "date_fichier_parsed",
    to_date(col("date_fichier"), "yyyyMMdd")
).withColumn(
    "Numero_Semaine",
    concat(
        year(col("date_fichier_parsed")).cast("string"),
        lpad(weekofyear(col("date_fichier_parsed")).cast("string"), 2, "0")
    ).cast(IntegerType())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. Conversion des colonnes vers les bons types + trim sur les VARCHAR
# =============================================================================
df_silver = df.select(
    col("Numero_Semaine"),
    trim(col("Code_GPS")).alias("Code_GPS"),
    col("Identifiant_technique_de_la_tache").cast(IntegerType()).alias("Identifiant_technique_de_la_tache"),
    trim(col("GPS_CICAS")).alias("GPS_CICAS"),
    trim(col("Libelle_EG_corbeille_Collective")).alias("Libelle_EG_corbeille_Collective"),
    trim(col("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache")).alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),
    trim(col("Libelle_long_Type_Tache_Metier")).alias("Libelle_long_Type_Tache_Metier"),
    to_timestamp(col("Timestamp_creation_tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_creation_tache"),
    trim(col("Libelle_long_Statut_Tache")).alias("Libelle_long_Statut_Tache"),
    to_timestamp(col("Timestamp_statut_tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_statut_tache"),
    col("Poids_de_la_tache").cast(ShortType()).alias("Poids_de_la_tache"),
    to_date(col("Date_limite_de_traitement"), "yyyy-MM-dd").alias("Date_limite_de_traitement"),
    trim(col("Sensibilite")).alias("Sensibilite"),
    trim(col("Identifiant_du_dossier")).alias("Identifiant_du_dossier"),
    trim(col("Libelle_IRC_paiement")).alias("Libelle_IRC_paiement"),
    trim(col("Libelle_court_Sous_processus_metier")).alias("Libelle_court_Sous_processus_metier"),
    trim(col("Libelle_Type_Droit")).alias("Libelle_Type_Droit"),
    trim(col("Libelle_Type_Stock_Dossier")).alias("Libelle_Type_Stock_Dossier"),
    trim(col("Libelle_Couleur_Dossier")).alias("Libelle_Couleur_Dossier"),
    to_date(col("Date_RDV_Dossier"), "yyyy-MM-dd").alias("Date_RDV_Dossier"),
    to_date(col("Date_d_effet_retenue_du_dossier"), "yyyy-MM-dd").alias("Date_d_effet_retenue_du_dossier"),
    to_timestamp(col("Date_paiement_provisoire"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Date_paiement_provisoire"),
    trim(col("Type_depot")).alias("Type_depot"),
    trim(col("Perimetre")).alias("Perimetre"),
    to_date(col("Date_limite_COM"), "yyyy-MM-dd").alias("Date_limite_COM"),
    to_date(col("Date_limite_COM_apres_provisoire"), "yyyy-MM-dd").alias("Date_limite_COM_apres_provisoire"),
    trim(col("IRC_primo_affectation")).alias("IRC_primo_affectation"),
    trim(col("BriqueSysteme_applicatif")).alias("BriqueSysteme_applicatif"),
    current_timestamp().alias("integration_ts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 4. Chargement dans la table Silver (delete + append par Numero_Semaine)
# =============================================================================

# Récupérer les valeurs distinctes de Numero_Semaine à charger
semaines = [row.Numero_Semaine for row in df_silver.select("Numero_Semaine").distinct().collect()]
semaines_str = ",".join([str(s) for s in semaines])

# Supprimer les lignes existantes avec les mêmes Numero_Semaine via SQL
spark.sql(f"DELETE FROM dp_scl_tache WHERE Numero_Semaine IN ({semaines_str})")

# Insérer les nouvelles données en mode append
df_silver.write \
    .format("delta") \
    .mode("append") \
    .insertInto("dp_scl_tache")

print(f"Semaines traitées : {semaines}")
print(f"Nombre de lignes total : {spark.table('dp_scl_tache').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
