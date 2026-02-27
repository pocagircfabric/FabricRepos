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
df_bronze = spark.table("lkh_bronze.dp_brc_cfiab_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Extraction du numéro de semaine depuis le nom du fichier
#    Exemple filename : PSID099.CMN.BRC.ListeDetail.Stock-Entrees-Sorties-Taches-HEBDO.20251116.csv
#    date_fichier = 20251116 (ssaammjj)
#    Numero_Semaine = 202546 (ssaann)
# =============================================================================
df = df_bronze.withColumn(
    "date_fichier",
    regexp_extract(col("filename"), r"(\d{8})\.\w+$", 1)
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
    col("Identifiant_technique_tache").cast(IntegerType()).alias("Identifiant_technique_tache"),
    trim(col("IRC_primo_affectation_tache")).alias("IRC_primo_affectation_tache"),
    trim(col("Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation")).alias("Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation"),
    trim(col("Libelle_EG_corbeille_Collective_d_affectation")).alias("Libelle_EG_corbeille_Collective_d_affectation"),
    trim(col("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache")).alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),
    trim(col("Systeme_applicatif")).alias("Systeme_applicatif"),
    trim(col("Libelle_court_Processus_Metier")).alias("Libelle_court_Processus_Metier"),
    trim(col("Libelle_court_Sous_processus_Metier")).alias("Libelle_court_Sous_processus_Metier"),
    trim(col("Libelle_long_Etape_Metier")).alias("Libelle_long_Etape_Metier"),
    trim(col("Code_type_tache")).alias("Code_type_tache"),
    trim(col("Libelle_long_Type_Tache_Metier")).alias("Libelle_long_Type_Tache_Metier"),
    to_timestamp(col("Timestamp_creation_tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_creation_tache"),
    to_timestamp(col("Timestamp_Affectation_tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_Affectation_tache"),
    trim(col("Libelle_long_Statut_Tache")).alias("Libelle_long_Statut_Tache"),
    to_timestamp(col("Timestamp_Statut_tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_Statut_tache"),
    col("Poids_de_la_tache").cast(IntegerType()).alias("Poids_de_la_tache"),
    col("Degre_d_urgence_Tache").cast(IntegerType()).alias("Degre_d_urgence_Tache"),
    to_date(col("Date_limite_de_traitement"), "yyyy-MM-dd").alias("Date_limite_de_traitement"),
    trim(col("Sensibilite")).alias("Sensibilite"),
    trim(col("Identifiant_interne_Type_Objet_Gestion_Distribution")).alias("Identifiant_interne_Type_Objet_Gestion_Distribution"),
    trim(col("Identifiant_interne_Type_Objet_Gestion_Reference")).alias("Identifiant_interne_Type_Objet_Gestion_Reference"),
    trim(col("Identifiant_Interne_Type_Objet_Dossier")).alias("Identifiant_Interne_Type_Objet_Dossier"),
    trim(col("Tache_en_Stock")).alias("Tache_en_Stock"),
    trim(col("Tache_en_entree")).alias("Tache_en_entree"),
    trim(col("Tache_en_Sortie")).alias("Tache_en_Sortie"),
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
spark.sql(f"DELETE FROM dp_brc_global WHERE Numero_Semaine IN ({semaines_str})")

# Insérer les nouvelles données en mode append
df_silver.write \
    .format("delta") \
    .mode("append") \
    .insertInto("dp_brc_global")

print(f"Semaines traitées : {semaines}")
print(f"Nombre de lignes total : {spark.table('dp_brc_global').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
