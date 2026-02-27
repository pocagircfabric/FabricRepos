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
# META     "environment": {
# META       "environmentId": "8c2325d8-3f7a-b1b8-4f9b-084cd2518e9f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

#df = spark.read.format("csv").option("header","true").option("delimiter",";").load("Files/OneDrive_1_2-26-2026/PSID099.CMN.SCL.ListeDetail.LIQ02-SuiviTachesLiquidation-Stock-HEBDO.20251109.csv")

schema = StructType([
    StructField('Code_GPS', StringType(), True),
    StructField('Identifiant_technique_de_la_tache', StringType(), True),
    StructField('GPS_CICAS', StringType(), True),
    StructField('Libelle_EG_corbeille_Collective', StringType(), True),
    StructField('Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache', StringType(), True),
    StructField('Libelle_long_Type_Tache_Metier', StringType(), True),
    StructField('Timestamp_creation_tache', StringType(), True),
    StructField('Libelle_long_Statut_Tache', StringType(), True),
    StructField('Timestamp_statut_tache', StringType(), True),
    StructField('Poids_de_la_tache', StringType(), True),
    StructField('Date_limite_de_traitement', StringType(), True),
    StructField('Sensibilite', StringType(), True),
    StructField('Identifiant_du_dossier', StringType(), True),
    StructField('Libelle_IRC_paiement', StringType(), True),
    StructField('Libelle_court_Sous_processus_metier', StringType(), True),
    StructField('Libelle_Type_Droit', StringType(), True),
    StructField('Libelle_Type_Stock_Dossier', StringType(), True),
    StructField('Libelle_Couleur_Dossier', StringType(), True),
    StructField('Date_RDV_Dossier', StringType(), True),
    StructField('Date_d_effet_retenue_du_dossier', StringType(), True),
    StructField('Date_paiement_provisoire', StringType(), True),
    StructField('Type_depot', StringType(), True),
    StructField('Perimetre', StringType(), True),
    StructField('Date_limite_COM', StringType(), True),
    StructField('Date_limite_COM_apres_provisoire', StringType(), True),
    StructField('IRC_primo_affectation', StringType(), True),
    StructField('BriqueSysteme_applicatif', StringType(), True)
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
        .load("Files/OneDrive_1_2-26-2026/PSID099.CMN.SCL.ListeDetail.LIQ02-SuiviTachesLiquidation-Stock-HEBDO*.csv")
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

df.write.format("delta").mode("overwrite").save("Tables/dp_scl_tache_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
