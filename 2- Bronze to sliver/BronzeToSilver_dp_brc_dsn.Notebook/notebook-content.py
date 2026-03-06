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
df_bronze = spark.table("lkh_bronze.dp_brc_dsn_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Extraction du numéro de semaine depuis le nom du fichier
#    Exemple filename : PSID099.CMN.BRC.ListeDetail.Entreprise-DSN-Stock-Entrees-Sorties-Taches-HEBDO.20251102.csv
#    date_fichier = 20251102 (ssaammjj)
#    Numero_Semaine = 202544 (ssaann)
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
    trim(col("Identifiant_fonctionnel_IRC_Tache")).alias("Identifiant_fonctionnel_IRC_Tache"),
    trim(col("Code_type_tache")).alias("Code_type_tache"),
    trim(col("Libelle_long_type_tache")).alias("Libelle_long_type_tache"),
    trim(col("Libelle_long_Statut_Tache")).alias("Libelle_long_Statut_Tache"),
    trim(col("Chemin_arborescence_nom_EG_Affectation")).alias("Chemin_arborescence_nom_EG_Affectation"),
    trim(col("Anonymisation")).alias("Anonymisation"),
    to_date(col("Date_limite_de_traitement"), "yyyy-MM-dd").alias("Date_limite_de_traitement"),
    to_timestamp(col("Date_de_creation_Tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Date_de_creation_Tache"),
    to_timestamp(col("Date_statut_Tache"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Date_statut_Tache"),
    trim(col("Libelle_information_1")).alias("Libelle_information_1"),
    trim(col("Libelle_information_2")).alias("Libelle_information_2"),
    trim(col("Libelle_information_3")).alias("Libelle_information_3"),
    trim(col("Libelle_information_complementaire_nombre_1")).alias("Libelle_information_complementaire_nombre_1"),
    col("Valeur_information_complementaire_nombre_1").cast(IntegerType()).alias("Valeur_information_complementaire_nombre_1"),
    trim(col("Libelle_information_complementaire_nombre_2")).alias("Libelle_information_complementaire_nombre_2"),
    trim(col("Valeur_information_complementaire_nombre_2")).alias("Valeur_information_complementaire_nombre_2"),
    trim(col("Libelle_information_complementaire_date_1")).alias("Libelle_information_complementaire_date_1"),
    trim(col("Valeur_information_complementaire_date_1")).alias("Valeur_information_complementaire_date_1"),
    col("Indicateur_presence_libelle_commentaire_utilisateur").cast(ShortType()).alias("Indicateur_presence_libelle_commentaire_utilisateur"),
    trim(col("SIREN")).alias("SIREN"),
    trim(col("Indicateur_Grande_entreprise")).alias("Indicateur_Grande_entreprise"),
    col("Nombre_d_adhesions_ouvertes").cast(ShortType()).alias("Nombre_d_adhesions_ouvertes"),
    trim(col("Indicateur_entreprise_protegee")).alias("Indicateur_entreprise_protegee"),
    col("Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice").cast(IntegerType()).alias("Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice"),
    col("Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice").cast(IntegerType()).alias("Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice"),
    col("Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice").cast(IntegerType()).alias("Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice"),
    trim(col("Tache_en_stock")).alias("Tache_en_stock"),
    trim(col("Tache_en_entree")).alias("Tache_en_entree"),
    trim(col("Tache_en_sortie")).alias("Tache_en_sortie"),
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
spark.sql(f"DELETE FROM dp_brc_dsn WHERE Numero_Semaine IN ({semaines_str})")

# Insérer les nouvelles données en mode append
df_silver.write \
    .format("delta") \
    .mode("append") \
    .insertInto("dp_brc_dsn")

print(f"Semaines traitées : {semaines}")
print(f"Nombre de lignes total : {spark.table('dp_brc_dsn').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
