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
df_bronze = spark.table("lkh_bronze.dp_scl_dossier_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Extraction du numéro de semaine depuis le nom du fichier
#    Exemple filename : ...PSID099.CMN.SCL.ListeDetail.LIQ01-SuiviDossiersLiquidation-Stock-HEBDO.20251123.csv?version=...
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
display(df)

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
    trim(col("Identifiant_du_dossier")).alias("Identifiant_du_dossier"),
    trim(col("Libelle_GPS_paiement")).alias("Libelle_GPS_paiement"),
    trim(col("Libelle_IRC_paiement")).alias("Libelle_IRC_paiement"),
    to_date(col("Date_effet_retenue"), "yyyy-MM-dd").alias("Date_effet_retenue"),
    trim(col("Libelle_Motif_Retraite")).alias("Libelle_Motif_Retraite"),
    trim(col("Type_depot")).alias("Type_depot"),
    trim(col("Libelle_Canal_Origine")).alias("Libelle_Canal_Origine"),
    trim(col("Libelle_lieu_Residence")).alias("Libelle_lieu_Residence"),
    trim(col("Libelle_Type_Droit")).alias("Libelle_Type_Droit"),
    trim(col("Libelle_Type_Retraite")).alias("Libelle_Type_Retraite"),
    trim(col("Libelle_Perimetre")).alias("Libelle_Perimetre"),
    trim(col("Derniere_Colorisation_du_dossier")).alias("Derniere_Colorisation_du_dossier"),
    to_timestamp(col("Timestamp_reception_notification_CNAV_MSA_RSI"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_reception_notification_CNAV_MSA_RSI"),
    to_timestamp(col("Date_reception_DLR"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Date_reception_DLR"),
    to_date(col("Date_reception_preuve_etat_civil"), "yyyy-MM-dd").alias("Date_reception_preuve_etat_civil"),
    to_date(col("Date_reception_RIB"), "yyyy-MM-dd").alias("Date_reception_RIB"),
    to_timestamp(col("Timestamp_evenement_Demande_de_paiement_provisoire"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_evenement_Demande_de_paiement_provisoire"),
    to_timestamp(col("Timestamp_evenement_de_mise_en_paiement_provisoire"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_evenement_de_mise_en_paiement_provisoire"),
    to_timestamp(col("Timestamp_evenement_Demande_de_paiement_definitif"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_evenement_Demande_de_paiement_definitif"),
    col("Nombre_de_points_inscrits").cast(IntegerType()).alias("Nombre_de_points_inscrits"),
    trim(col("Corbeille_individuelle_derniere_tache_Constitution")).alias("Corbeille_individuelle_derniere_tache_Constitution"),
    trim(col("Libelle_Chemin_Arborescence_derniere_tache_Constitution")).alias("Libelle_Chemin_Arborescence_derniere_tache_Constitution"),
    to_date(col("Date_limite_COM"), "yyyy-MM-dd").alias("Date_limite_COM"),
    to_date(col("Date_limite_COM_apres_provisoire"), "yyyy-MM-dd").alias("Date_limite_COM_apres_provisoire"),
    trim(col("Presence_dossier_IRCANTEC")).alias("Presence_dossier_IRCANTEC"),
    trim(col("Libelle_long_motif_couleur_Carriere")).alias("Libelle_long_motif_couleur_Carriere"),
    trim(col("Libelle_long_motif_couleur_Calcul_Droits")).alias("Libelle_long_motif_couleur_Calcul_Droits"),
    trim(col("Libelle_long_motif_couleur_Synthese_Individu")).alias("Libelle_long_motif_couleur_Synthese_Individu"),
    trim(col("Libelle_long_motif_couleur_Demande_de_retraite")).alias("Libelle_long_motif_couleur_Demande_de_retraite"),
    trim(col("Motif_colorisation_onglet_Famille")).alias("Motif_colorisation_onglet_Famille"),
    col("Nombre_de_pieces_a_traiter").cast(ShortType()).alias("Nombre_de_pieces_a_traiter"),
    trim(col("Presence_tache_en_cours")).alias("Presence_tache_en_cours"),
    trim(col("Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation")).alias("Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation"),
    trim(col("Libelle_EG_corbeille_Collective_d_affectation")).alias("Libelle_EG_corbeille_Collective_d_affectation"),
    trim(col("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache")).alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),
    trim(col("Complexite_carriere")).alias("Complexite_carriere"),
    col("Nombre_de_preuves_en_attente").cast(ShortType()).alias("Nombre_de_preuves_en_attente"),
    to_timestamp(col("Timestamp_Envoi_Courrier_PMR"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("Timestamp_Envoi_Courrier_PMR"),
    trim(col("Identifiant_individu_de_Communication")).alias("Identifiant_individu_de_Communication"),
    col("Flag_Stock_non_paye").cast(ShortType()).alias("Flag_Stock_non_paye"),
    col("Flag_Stock_provisoire").cast(ShortType()).alias("Flag_Stock_provisoire"),
    to_date(col("Date_CER_Integral"), "yyyy-MM-dd").alias("Date_CER_Integral"),
    trim(col("Code_Liquidation_Tous_Regime")).alias("Code_Liquidation_Tous_Regime"),
    trim(col("Libelle_Statut_PSAA")).alias("Libelle_Statut_PSAA"),
    trim(col("Corbeille_individuelle_derniere_tache_Etude")).alias("Corbeille_individuelle_derniere_tache_Etude"),
    trim(col("Libelle_Chemin_Arborescence_derniere_tache_Etudes")).alias("Libelle_Chemin_Arborescence_derniere_tache_Etudes"),
    trim(col("Statut_Tache_Dossier")).alias("Statut_Tache_Dossier"),
    trim(col("Type_Tache_Dossier")).alias("Type_Tache_Dossier"),
    trim(col("Affectation_Dossier")).alias("Affectation_Dossier"),
    trim(col("Indicateur_Pieces_a_traiter")).alias("Indicateur_Pieces_a_traiter"),
    trim(col("Indicateur_Absence_de_tache")).alias("Indicateur_Absence_de_tache"),
    trim(col("Indicateur_NPV_recue")).alias("Indicateur_NPV_recue"),
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
# Configuration pour gérer les dates anciennes
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Récupérer les valeurs distinctes de Numero_Semaine à charger
semaines = [row.Numero_Semaine for row in df_silver.select("Numero_Semaine").distinct().collect()]
semaines_str = ",".join([str(s) for s in semaines])

# Supprimer les lignes existantes avec les mêmes Numero_Semaine via SQL
spark.sql(f"DELETE FROM dp_scl_dossier WHERE Numero_Semaine IN ({semaines_str})")

# Insérer les nouvelles données en mode append
df_silver.write \
    .format("delta") \
    .mode("append") \
    .insertInto("dp_scl_dossier")

print(f"Semaines traitées : {semaines}")
print(f"Nombre de lignes total : {spark.table('dp_scl_dossier').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
