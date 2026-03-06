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

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, ShortType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 1. Lecture des tables Silver sources
# =============================================================================
df_dsn    = spark.table("dp_brc_dsn")
df_global = spark.table("dp_brc_global")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Jointure DSN x GLOBAL sur [Numero_Semaine, Code_GPS, Identifiant_technique_tache]
#    + Filtre : Tache_en_Stock = 'OUI'
# =============================================================================
join_keys = ["Numero_Semaine", "Code_GPS", "Identifiant_technique_tache"]

df_joined = (
    df_dsn.alias("dsn")
    .join(df_global.alias("glb"), on=join_keys, how="inner")
    .filter(col("dsn.Tache_en_stock") == "OUI")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. Sélection des colonnes cibles pour dp_brc_dsn_stock_tache
#    - Colonnes DSN    : Identifiant_fonctionnel_IRC_Tache, Code_type_tache,
#                        Libelle_long_type_tache, Libelle_long_Statut_Tache,
#                        Chemin_arborescence_nom_EG_Affectation,
#                        Date_limite_de_traitement, Libelle_information_1/2/3,
#                        Infos complémentaires nombre 1/2, date 1,
#                        SIREN, Indicateur_Grande_entreprise, etc.
#    - Colonnes GLOBAL : Systeme_applicatif, Processus/Sous-processus/Etape,
#                        Timestamps (création, affectation, statut),
#                        Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache,
#                        Poids_de_la_tache, Degre_d_urgence_Tache, Sensibilite
# =============================================================================
df_silver = df_joined.select(
    # --- Clés de jointure ---
    col("Numero_Semaine"),
    col("Code_GPS"),
    col("Identifiant_technique_tache"),

    # --- DSN ---
    col("dsn.Identifiant_fonctionnel_IRC_Tache").alias("Identifiant_fonctionnel_IRC_Tache"),

    # --- GLOBAL ---
    col("glb.Systeme_applicatif").alias("Systeme_applicatif"),
    col("glb.Libelle_court_Processus_Metier").alias("Libelle_court_Processus_Metier"),
    col("glb.Libelle_court_Sous_processus_Metier").alias("Libelle_court_Sous_processus_Metier"),
    col("glb.Libelle_long_Etape_Metier").alias("Libelle_long_Etape_Metier"),

    # --- DSN ---
    col("dsn.Code_type_tache").alias("Code_type_tache"),
    col("dsn.Libelle_long_type_tache").alias("Libelle_long_type_tache"),

    # --- GLOBAL : Timestamps ---
    col("glb.Timestamp_creation_tache").alias("Timestamp_creation_tache"),
    col("glb.Timestamp_Affectation_tache").alias("Timestamp_Affectation_tache"),
    col("glb.Timestamp_Statut_tache").alias("Timestamp_Statut_tache"),

    # --- DSN ---
    col("dsn.Libelle_long_Statut_Tache").alias("Libelle_long_Statut_Tache"),
    col("dsn.Chemin_arborescence_nom_EG_Affectation").alias("Chemin_arborescence_nom_EG_Affectation"),

    # --- GLOBAL ---
    col("glb.Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache").alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),

    # --- DSN ---
    col("dsn.Date_limite_de_traitement").alias("Date_limite_de_traitement"),
    col("dsn.Libelle_information_1").alias("Libelle_information_1"),
    col("dsn.Libelle_information_2").alias("Libelle_information_2"),
    col("dsn.Libelle_information_3").alias("Libelle_information_3"),
    col("dsn.Libelle_information_complementaire_nombre_1").alias("Libelle_information_complementaire_nombre_1"),
    col("dsn.Valeur_information_complementaire_nombre_1").alias("Valeur_information_complementaire_nombre_1"),
    col("dsn.Libelle_information_complementaire_nombre_2").alias("Libelle_information_complementaire_nombre_2"),
    col("dsn.Valeur_information_complementaire_nombre_2").alias("Valeur_information_complementaire_nombre_2"),
    col("dsn.Libelle_information_complementaire_date_1").alias("Libelle_information_complementaire_date_1"),
    col("dsn.Valeur_information_complementaire_date_1").alias("Valeur_information_complementaire_date_1"),
    col("dsn.SIREN").alias("SIREN"),
    col("dsn.Indicateur_Grande_entreprise").alias("Indicateur_Grande_entreprise"),
    col("dsn.Nombre_d_adhesions_ouvertes").cast(ShortType()).alias("Nombre_d_adhesions_ouvertes"),
    col("dsn.Indicateur_entreprise_protegee").alias("Indicateur_entreprise_protegee"),
    col("dsn.Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice").alias("Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice"),
    col("dsn.Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice").alias("Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice"),
    col("dsn.Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice").alias("Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice"),

    # --- GLOBAL ---
    col("glb.Poids_de_la_tache").cast(ShortType()).alias("Poids_de_la_tache"),
    col("glb.Degre_d_urgence_Tache").cast(ShortType()).alias("Degre_d_urgence_Tache"),
    col("glb.Sensibilite").alias("Sensibilite"),

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
# 4. Chargement dans la table Silver (overwrite complet)
# =============================================================================

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dp_brc_dsn_stock_tache")

print(f"Nombre de lignes total : {spark.table('dp_brc_dsn_stock_tache').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
