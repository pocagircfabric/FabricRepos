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
from pyspark.sql.types import ShortType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 1. Lecture des tables Silver sources
# =============================================================================
df_dossier = spark.table("dp_scl_dossier")
df_tache   = spark.table("dp_scl_tache")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Sous-requête : calcul de Nombre_tache_en_stock par dossier et semaine
#    COUNT des tâches dans DP_SCL_TACHE groupé par [Numero_Semaine, Identifiant_du_dossier]
# =============================================================================
df_nb_taches = (
    df_tache
    .groupBy("Numero_Semaine", "Identifiant_du_dossier")
    .agg(
        count("Identifiant_technique_de_la_tache").cast(ShortType()).alias("Nombre_tache_en_stock")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. Jointure DP_SCL_DOSSIER avec le calcul Nombre_tache_en_stock
#    LEFT JOIN pour conserver les dossiers sans tâche (Nombre_tache_en_stock = 0)
# =============================================================================
from pyspark.sql.functions import coalesce, lit

df_joined = (
    df_dossier.alias("dos")
    .join(
        df_nb_taches.alias("tch"),
        on=["Numero_Semaine", "Identifiant_du_dossier"],
        how="left"
    )
    .withColumn(
        "Nombre_tache_en_stock",
        coalesce(col("tch.Nombre_tache_en_stock"), lit(0)).cast(ShortType())
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 4. Sélection des colonnes cibles pour dp_scl_dossier_stock_synthese
# =============================================================================
df_silver = df_joined.select(
    col("Numero_Semaine"),
    col("dos.Code_GPS").alias("Code_GPS"),
    col("Identifiant_du_dossier"),
    col("dos.Identifiant_individu_de_Communication").alias("Identifiant_individu_de_Communication"),
    col("dos.Libelle_GPS_paiement").alias("Libelle_GPS_paiement"),
    col("dos.Libelle_IRC_paiement").alias("Libelle_IRC_paiement"),
    col("dos.Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation").alias("Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation"),
    col("dos.Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache").alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),
    col("dos.Date_effet_retenue").alias("Date_effet_retenue"),
    col("dos.Libelle_Motif_Retraite").alias("Libelle_Motif_Retraite"),
    col("dos.Type_depot").alias("Type_depot"),
    col("dos.Libelle_Canal_Origine").alias("Libelle_Canal_Origine"),
    col("dos.Libelle_lieu_Residence").alias("Libelle_lieu_Residence"),
    col("dos.Libelle_Type_Droit").alias("Libelle_Type_Droit"),
    col("dos.Libelle_Type_Retraite").alias("Libelle_Type_Retraite"),
    col("dos.Derniere_Colorisation_du_dossier").alias("Derniere_Colorisation_du_dossier"),
    col("dos.Libelle_long_motif_couleur_Carriere").alias("Libelle_long_motif_couleur_Carriere"),
    col("dos.Libelle_long_motif_couleur_Calcul_Droits").alias("Libelle_long_motif_couleur_Calcul_Droits"),
    col("dos.Libelle_long_motif_couleur_Synthese_Individu").alias("Libelle_long_motif_couleur_Synthese_Individu"),
    col("dos.Libelle_long_motif_couleur_Demande_de_retraite").alias("Libelle_long_motif_couleur_Demande_de_retraite"),
    col("dos.Motif_colorisation_onglet_Famille").alias("Motif_colorisation_onglet_Famille"),
    col("dos.Timestamp_reception_notification_CNAV_MSA_RSI").alias("Timestamp_reception_notification_CNAV_MSA_RSI"),
    col("dos.Date_reception_DLR").alias("Date_reception_DLR"),
    col("dos.Date_reception_preuve_etat_civil").alias("Date_reception_preuve_etat_civil"),
    col("dos.Date_reception_RIB").alias("Date_reception_RIB"),
    col("dos.Timestamp_evenement_Demande_de_paiement_provisoire").alias("Timestamp_evenement_Demande_de_paiement_provisoire"),
    col("dos.Timestamp_evenement_de_mise_en_paiement_provisoire").alias("Timestamp_evenement_de_mise_en_paiement_provisoire"),
    col("dos.Timestamp_evenement_Demande_de_paiement_definitif").alias("Timestamp_evenement_Demande_de_paiement_definitif"),
    col("dos.Nombre_de_points_inscrits").alias("Nombre_de_points_inscrits"),
    col("dos.Presence_dossier_IRCANTEC").alias("Presence_dossier_IRCANTEC"),
    col("dos.Nombre_de_pieces_a_traiter").alias("Nombre_de_pieces_a_traiter"),
    col("Nombre_tache_en_stock"),
    col("dos.Complexite_carriere").alias("Complexite_carriere"),
    col("dos.Nombre_de_preuves_en_attente").alias("Nombre_de_preuves_en_attente"),
    col("dos.Flag_Stock_non_paye").alias("Flag_Stock_non_paye"),
    col("dos.Flag_Stock_provisoire").alias("Flag_Stock_provisoire"),
    col("dos.Indicateur_Pieces_a_traiter").alias("Indicateur_Pieces_a_traiter"),
    col("dos.Indicateur_NPV_recue").alias("Indicateur_NPV_recue"),

    # --- Colonne technique ---
    current_timestamp().alias("integration_ts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 5. Chargement dans la table Silver (overwrite complet)
# =============================================================================

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dp_scl_dossier_stock_synthese")

print(f"Nombre de lignes total : {spark.table('dp_scl_dossier_stock_synthese').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
