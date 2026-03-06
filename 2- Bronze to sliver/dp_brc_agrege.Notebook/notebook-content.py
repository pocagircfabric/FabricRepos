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

from pyspark.sql.functions import col, count, when, current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 1. Lecture de la table Silver source
# =============================================================================
df_global = spark.table("dp_brc_global")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Agrégation de DP_BRC_GLOBAL
#    Critères d'agrégation : 12 colonnes
#    Indicateurs :
#      NB_TACHE_STOCK  = COUNT(Identifiant_technique_tache) WHERE Tache_en_Stock  = 'OUI'
#      NB_TACHE_ENTREE = COUNT(Identifiant_technique_tache) WHERE Tache_en_entree = 'OUI'
#      NB_TACHE_SORTIE = COUNT(Identifiant_technique_tache) WHERE Tache_en_Sortie = 'OUI'
# =============================================================================

group_cols = [
    "Numero_Semaine",
    "Code_GPS",
    "IRC_primo_affectation_tache",
    "Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation",
    "Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache",
    "Systeme_applicatif",
    "Libelle_court_Processus_Metier",
    "Libelle_court_Sous_processus_Metier",
    "Libelle_long_Etape_Metier",
    "Code_type_tache",
    "Libelle_long_Type_Tache_Metier",
    "Libelle_long_Statut_Tache"
]

df_silver = (
    df_global
    .groupBy(group_cols)
    .agg(
        count(when(col("Tache_en_Stock") == "OUI", col("Identifiant_technique_tache"))).alias("NB_TACHE_STOCK"),
        count(when(col("Tache_en_entree") == "OUI", col("Identifiant_technique_tache"))).alias("NB_TACHE_ENTREE"),
        count(when(col("Tache_en_Sortie") == "OUI", col("Identifiant_technique_tache"))).alias("NB_TACHE_SORTIE")
    )
    .withColumn("integration_ts", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. Chargement dans la table Silver (overwrite complet)
# =============================================================================

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dp_brc_agrege")

print(f"Nombre de lignes total : {spark.table('dp_brc_agrege').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
