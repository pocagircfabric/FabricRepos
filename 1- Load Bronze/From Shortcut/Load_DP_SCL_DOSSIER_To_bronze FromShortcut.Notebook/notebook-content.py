# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b668c256-dda4-4864-ac30-b1c7f13a99ac",
# META       "default_lakehouse_name": "lkh_bronze",
# META       "default_lakehouse_workspace_id": "d500bac7-7518-4386-b1f8-857ca400e400",
# META       "known_lakehouses": [
# META         {
# META           "id": "b668c256-dda4-4864-ac30-b1c7f13a99ac"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType



schema = StructType([
    StructField('Code_GPS', StringType(), True),
    StructField('Identifiant_du_dossier', StringType(), True),
    StructField('Libelle_GPS_paiement', StringType(), True),
    StructField('Libelle_IRC_paiement', StringType(), True),
    StructField('Date_effet_retenue', StringType(), True),
    StructField('Libelle_Motif_Retraite', StringType(), True),
    StructField('Type_depot', StringType(), True),
    StructField('Libelle_Canal_Origine', StringType(), True),
    StructField('Libelle_lieu_Residence', StringType(), True),
    StructField('Libelle_Type_Droit', StringType(), True),
    StructField('Libelle_Type_Retraite', StringType(), True),
    StructField('Libelle_Perimetre', StringType(), True),
    StructField('Derniere_Colorisation_du_dossier', StringType(), True),
    StructField('Timestamp_reception_notification_CNAV_MSA_RSI', StringType(), True),
    StructField('Date_reception_DLR', StringType(), True),
    StructField('Date_reception_preuve_etat_civil', StringType(), True),
    StructField('Date_reception_RIB', StringType(), True),
    StructField('Timestamp_evenement_Demande_de_paiement_provisoire', StringType(), True),
    StructField('Timestamp_evenement_de_mise_en_paiement_provisoire', StringType(), True),
    StructField('Timestamp_evenement_Demande_de_paiement_definitif', StringType(), True),
    StructField('Nombre_de_points_inscrits', StringType(), True),
    StructField('Corbeille_individuelle_derniere_tache_Constitution', StringType(), True),
    StructField('Libelle_Chemin_Arborescence_derniere_tache_Constitution', StringType(), True),
    StructField('Date_limite_COM', StringType(), True),
    StructField('Date_limite_COM_apres_provisoire', StringType(), True),
    StructField('Presence_dossier_IRCANTEC', StringType(), True),
    StructField('Libelle_long_motif_couleur_Carriere', StringType(), True),
    StructField('Libelle_long_motif_couleur_Calcul_Droits', StringType(), True),
    StructField('Libelle_long_motif_couleur_Synthese_Individu', StringType(), True),
    StructField('Libelle_long_motif_couleur_Demande_de_retraite', StringType(), True),
    StructField('Motif_colorisation_onglet_Famille', StringType(), True),
    StructField('Nombre_de_pieces_a_traiter', StringType(), True),
    StructField('Presence_tache_en_cours', StringType(), True),
    StructField('Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation', StringType(), True),
    StructField('Libelle_EG_corbeille_Collective_d_affectation', StringType(), True),
    StructField('Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache', StringType(), True),
    StructField('Complexite_carriere', StringType(), True),
    StructField('Nombre_de_preuves_en_attente', StringType(), True),
    StructField('Timestamp_Envoi_Courrier_PMR', StringType(), True),
    StructField('Identifiant_individu_de_Communication', StringType(), True),
    StructField('Flag_Stock_non_paye', StringType(), True),
    StructField('Flag_Stock_provisoire', StringType(), True),
    StructField('Date_CER_Integral', StringType(), True),
    StructField('Code_Liquidation_Tous_Regime', StringType(), True),
    StructField('Libelle_Statut_PSAA', StringType(), True),
    StructField('Corbeille_individuelle_derniere_tache_Etude', StringType(), True),
    StructField('Libelle_Chemin_Arborescence_derniere_tache_Etudes', StringType(), True),
    StructField('Statut_Tache_Dossier', StringType(), True),
    StructField('Type_Tache_Dossier', StringType(), True),
    StructField('Affectation_Dossier', StringType(), True),
    StructField('Indicateur_Pieces_a_traiter', StringType(), True),
    StructField('Indicateur_Absence_de_tache', StringType(), True),
    StructField('Indicateur_NPV_recue', StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import current_timestamp, input_file_name, date_format

df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .schema(schema)
        .option("encoding", "windows-1252")
        .load("Files/Raw-data/PSID099.CMN.SCL.ListeDetail.LIQ01-SuiviDossiersLiquidation-Stock-HEBDO*.csv")
)

df = df.withColumn("File_Name", input_file_name())

df = df.withColumn(
    "ingestion_ts",
    date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
)


display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").save("Tables/dp_scl_dossier_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
