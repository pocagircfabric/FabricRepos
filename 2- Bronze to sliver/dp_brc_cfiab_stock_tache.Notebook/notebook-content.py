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
df_cfiab  = spark.table("dp_brc_cfiab")
df_global = spark.table("dp_brc_global")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 2. Jointure CFIAB x GLOBAL sur [Numero_Semaine, Code_GPS, Identifiant_technique_tache]
#    + Filtre : Tache_en_Stock = 'OUI'
# =============================================================================
join_keys = ["Numero_Semaine", "Code_GPS", "Identifiant_technique_tache"]

df_joined = (
    df_cfiab.alias("cfiab")
    .join(df_global.alias("glb"), on=join_keys, how="inner")
    .filter(col("cfiab.Tache_en_Stock") == "OUI")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. Sélection des colonnes cibles pour dp_brc_cfiab_stock_tache
#    - Colonnes CFIAB  : Identifiant_fonctionnel_IRC_Tache, Code_type_tache,
#                        Libelle_long_type_tache, Libelle_long_Statut_Tache,
#                        Chemin_arborescence_nom_EG_Affectation,
#                        Date_limite_de_traitement, Anciennete_Tache,
#                        Infos complémentaires 1/2/3,
#                        Indicateur_entreprise_protegee,
#                        Identifiant_Public_Type_Objet_CTX2
#    - Colonnes GLOBAL : Systeme_applicatif, 
#                            Processus/Sous-processus/Etape,
#                        Timestamps (création, affectation, statut),
#                        Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache,
#                        Identifiant_Interne_Type_Objet_Dossier,
#                        Poids_de_la_tache, Degre_d_urgence_Tache, Sensibilite
# =============================================================================
df_silver = df_joined.select(
    # --- Clés de jointure (depuis CFIAB) ---
    col("Numero_Semaine"),
    col("Code_GPS"),
    col("Identifiant_technique_tache"),

    # --- Colonnes CFIAB ---
    col("cfiab.Identifiant_fonctionnel_IRC_Tache").alias("Identifiant_fonctionnel_IRC_Tache"),

    # --- Colonnes GLOBAL ---
    col("glb.Systeme_applicatif").alias("Systeme_applicatif"),
    col("glb.Libelle_court_Processus_Metier").alias("Libelle_court_Processus_Metier"),
    col("glb.Libelle_court_Sous_processus_Metier").alias("Libelle_court_Sous_processus_Metier"),
    col("glb.Libelle_long_Etape_Metier").alias("Libelle_long_Etape_Metier"),

    # --- CFIAB ---
    col("cfiab.Code_type_tache").alias("Code_type_tache"),
    col("cfiab.Libelle_long_type_tache").alias("Libelle_long_type_tache"),

    # --- GLOBAL : Timestamps ---
    col("glb.Timestamp_creation_tache").alias("Timestamp_creation_tache"),
    col("glb.Timestamp_Affectation_tache").alias("Timestamp_Affectation_tache"),
    col("glb.Timestamp_Statut_tache").alias("Timestamp_Statut_tache"),

    # --- CFIAB ---
    col("cfiab.Libelle_long_Statut_Tache").alias("Libelle_long_Statut_Tache"),
    col("cfiab.Chemin_arborescence_nom_EG_Affectation").alias("Chemin_arborescence_nom_EG_Affectation"),

    # --- GLOBAL ---
    col("glb.Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache").alias("Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache"),

    # --- CFIAB ---
    col("cfiab.Date_limite_de_traitement").alias("Date_limite_de_traitement"),
    col("cfiab.Anciennete_Tache").alias("Anciennete_Tache"),
    col("cfiab.Libelle_information_complementaire_nombre_1").alias("Libelle_information_complementaire_nombre_1"),
    col("cfiab.Valeur_information_complementaire_nombre_1").alias("Valeur_information_complementaire_nombre_1"),
    col("cfiab.Libelle_information_complementaire_nombre_2").alias("Libelle_information_complementaire_nombre_2"),
    col("cfiab.Valeur_information_complementaire_nombre_2").alias("Valeur_information_complementaire_nombre_2"),
    col("cfiab.Libelle_information_complementaire_nombre_3").alias("Libelle_information_complementaire_nombre_3"),
    col("cfiab.Valeur_information_complementaire_nombre_3").alias("Valeur_information_complementaire_nombre_3"),
    col("cfiab.Indicateur_entreprise_protegee").alias("Indicateur_entreprise_protegee"),
    col("cfiab.Identifiant_Public_Type_Objet_CTX2").alias("Identifiant_Public_Type_Objet_CTX2"),

    # --- GLOBAL ---
    col("glb.Identifiant_Interne_Type_Objet_Dossier").alias("Identifiant_Interne_Type_Objet_Dossier"),
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
    .saveAsTable("dp_brc_cfiab_stock_tache")

print(f"Nombre de lignes total : {spark.table('dp_brc_cfiab_stock_tache').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
