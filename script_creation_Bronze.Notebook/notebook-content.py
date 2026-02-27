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

# MAGIC %%sql
# MAGIC /* ===========================================================
# MAGIC    FABRIC LAKEHOUSE - BRONZE LAYER (RAW)
# MAGIC    - Le Lakehouse = Bronze
# MAGIC    - 1 fichier = 1 table raw
# MAGIC    =========================================================== */
# MAGIC 
# MAGIC /* ---------- DP_BRC_GLOBAL ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_global_raw;
# MAGIC CREATE TABLE dp_brc_global_raw (
# MAGIC     Code_GPS                                                                    STRING,
# MAGIC     Identifiant_technique_tache                                                 STRING,
# MAGIC     IRC_primo_affectation_tache                                                 STRING,
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation        STRING,
# MAGIC     Libelle_EG_corbeille_Collective_d_affectation                               STRING,
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache                      STRING,
# MAGIC     Systeme_applicatif                                                          STRING,
# MAGIC     Libelle_court_Processus_Metier                                              STRING,
# MAGIC     Libelle_court_Sous_processus_Metier                                         STRING,
# MAGIC     Libelle_long_Etape_Metier                                                   STRING,
# MAGIC     Code_type_tache                                                             STRING,
# MAGIC     Libelle_long_Type_Tache_Metier                                              STRING,
# MAGIC     Timestamp_creation_tache                                                    STRING,
# MAGIC     Timestamp_Affectation_tache                                                 STRING,
# MAGIC     Libelle_long_Statut_Tache                                                   STRING,
# MAGIC     Timestamp_Statut_tache                                                      STRING,
# MAGIC     Poids_de_la_tache                                                           STRING,
# MAGIC     Degre_d_urgence_Tache                                                       STRING,
# MAGIC     Date_limite_de_traitement                                                   STRING,
# MAGIC     Sensibilite                                                                 STRING,
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Distribution                         STRING,
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Reference                            STRING,
# MAGIC     Identifiant_Interne_Type_Objet_Dossier                                      STRING,
# MAGIC     Tache_en_Stock                                                              STRING,
# MAGIC     Tache_en_entree                                                             STRING,
# MAGIC     Tache_en_Sortie                                                             STRING,
# MAGIC     File_Name                                                                   STRING,
# MAGIC     ingestion_ts                                                                STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_BRC_CFIAB ---------- */
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dp_brc_cfiab_raw;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_cfiab_raw (
# MAGIC     Code_GPS                                                STRING,
# MAGIC     Identifiant_technique_tache                             STRING,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                       STRING,
# MAGIC     Code_type_tache                                         STRING,
# MAGIC     Libelle_long_type_tache                                 STRING,
# MAGIC     Libelle_long_Statut_Tache                               STRING,
# MAGIC     Chemin_arborescence_nom_EG_Affectation                  STRING,
# MAGIC     Anonymisation                                           STRING,
# MAGIC     Date_limite_de_traitement                               STRING,
# MAGIC     Date_de_creation_Tache                                  STRING,
# MAGIC     Date_statut_Tache                                       STRING,
# MAGIC     Anciennete_Tache                                        STRING,
# MAGIC     Libelle_information_complementaire_nombre_1             STRING,
# MAGIC     Valeur_information_complementaire_nombre_1              STRING,
# MAGIC     Libelle_information_complementaire_nombre_2             STRING,
# MAGIC     Valeur_information_complementaire_nombre_2              STRING,
# MAGIC     Libelle_information_complementaire_nombre_3             STRING,
# MAGIC     Valeur_information_complementaire_nombre_3              STRING,
# MAGIC     Indicateur_presence_libelle_commentaire_utilisateur     STRING,
# MAGIC     Indicateur_entreprise_protegee                          STRING,
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Reference        STRING,
# MAGIC     Identifiant_Public_Type_Objet_CTX2                      STRING,
# MAGIC     Tache_en_Stock                                          STRING,
# MAGIC     Tache_en_entree                                         STRING,
# MAGIC     Tache_en_Sortie                                         STRING,
# MAGIC     File_Name                                               STRING,
# MAGIC     ingestion_ts                                            STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC /* ---------- DP_BRC_DSN ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_dsn_raw;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_dsn_raw (
# MAGIC     Code_GPS                                                                                STRING,
# MAGIC     Identifiant_technique_tache                                                             STRING,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                                                       STRING,
# MAGIC     Code_type_tache                                                                         STRING,
# MAGIC     Libelle_long_type_tache                                                                 STRING,
# MAGIC     Libelle_long_Statut_Tache                                                               STRING,
# MAGIC     Chemin_arborescence_nom_EG_Affectation                                                  STRING,
# MAGIC     Anonymisation                                                                           STRING,
# MAGIC     Date_limite_de_traitement                                                               STRING,
# MAGIC     Date_de_creation_Tache                                                                  STRING,
# MAGIC     Date_statut_Tache                                                                       STRING,
# MAGIC     Libelle_information_1                                                                   STRING,
# MAGIC     Libelle_information_2                                                                   STRING,
# MAGIC     Libelle_information_3                                                                   STRING,
# MAGIC     Libelle_information_complementaire_nombre_1                                             STRING,
# MAGIC     Valeur_information_complementaire_nombre_1                                              STRING,
# MAGIC     Libelle_information_complementaire_nombre_2                                             STRING,
# MAGIC     Valeur_information_complementaire_nombre_2                                              STRING,
# MAGIC     Libelle_information_complementaire_date_1                                               STRING,
# MAGIC     Valeur_information_complementaire_date_1                                                STRING,
# MAGIC     Indicateur_presence_libelle_commentaire_utilisateur                                     STRING,
# MAGIC     SIREN                                                                                   STRING,
# MAGIC     Indicateur_Grande_entreprise                                                            STRING,
# MAGIC     Nombre_d_adhesions_ouvertes                                                             STRING,
# MAGIC     Indicateur_entreprise_protegee                                                          STRING,
# MAGIC     Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice           STRING,
# MAGIC     Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice      STRING,
# MAGIC     Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice                       STRING,
# MAGIC     Tache_en_stock                                                                          STRING,
# MAGIC     Tache_en_entree                                                                         STRING,
# MAGIC     Tache_en_sortie                                                                         STRING,
# MAGIC     File_Name                                                                               STRING,
# MAGIC     ingestion_ts                                                                            STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_SCL_TACHE ---------- */
# MAGIC DROP TABLE IF EXISTS dp_scl_tache_raw;
# MAGIC 
# MAGIC CREATE TABLE dp_scl_tache_raw (
# MAGIC     Code_GPS                                                        STRING,
# MAGIC     Identifiant_technique_de_la_tache                               STRING,
# MAGIC     GPS_CICAS                                                       STRING,
# MAGIC     Libelle_EG_corbeille_Collective                                 STRING,
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache          STRING,
# MAGIC     Libelle_long_Type_Tache_Metier                                  STRING,
# MAGIC     Timestamp_creation_tache                                        STRING,
# MAGIC     Libelle_long_Statut_Tache                                       STRING,
# MAGIC     Timestamp_statut_tache                                          STRING,
# MAGIC     Poids_de_la_tache                                               STRING,
# MAGIC     Date_limite_de_traitement                                       STRING,
# MAGIC     Sensibilite                                                     STRING,
# MAGIC     Identifiant_du_dossier                                          STRING,
# MAGIC     Libelle_IRC_paiement                                            STRING,
# MAGIC     Libelle_court_Sous_processus_metier                             STRING,
# MAGIC     Libelle_Type_Droit                                              STRING,
# MAGIC     Libelle_Type_Stock_Dossier                                      STRING,
# MAGIC     Libelle_Couleur_Dossier                                         STRING,
# MAGIC     Date_RDV_Dossier                                                STRING,
# MAGIC     Date_d_effet_retenue_du_dossier                                 STRING,
# MAGIC     Date_paiement_provisoire                                        STRING,
# MAGIC     Type_depot                                                      STRING,
# MAGIC     Perimetre                                                       STRING,
# MAGIC     Date_limite_COM                                                 STRING,
# MAGIC     Date_limite_COM_apres_provisoire                                STRING,
# MAGIC     IRC_primo_affectation                                           STRING,
# MAGIC     BriqueSysteme_applicatif                                        STRING,
# MAGIC     File_Name                                                       STRING,
# MAGIC     ingestion_ts                                                    STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC /* ---------- DP_SCL_DOSSIER ---------- */
# MAGIC DROP TABLE IF EXISTS dp_scl_dossier_raw;
# MAGIC 
# MAGIC CREATE TABLE dp_scl_dossier_raw (
# MAGIC     Code_GPS                                                            STRING,
# MAGIC     Identifiant_du_dossier                                              STRING,
# MAGIC     Libelle_GPS_paiement                                                STRING,
# MAGIC     Libelle_IRC_paiement                                                STRING,
# MAGIC     Date_effet_retenue                                                  STRING,
# MAGIC     Libelle_Motif_Retraite                                              STRING,
# MAGIC     Type_depot                                                          STRING,
# MAGIC     Libelle_Canal_Origine                                               STRING,
# MAGIC     Libelle_lieu_Residence                                              STRING,
# MAGIC     Libelle_Type_Droit                                                  STRING,
# MAGIC     Libelle_Type_Retraite                                               STRING,
# MAGIC     Libelle_Perimetre                                                   STRING,
# MAGIC     Derniere_Colorisation_du_dossier                                    STRING,
# MAGIC     Timestamp_reception_notification_CNAV_MSA_RSI                       STRING,
# MAGIC     Date_reception_DLR                                                  STRING,
# MAGIC     Date_reception_preuve_etat_civil                                    STRING,
# MAGIC     Date_reception_RIB                                                  STRING,
# MAGIC     Timestamp_evenement_Demande_de_paiement_provisoire                  STRING,
# MAGIC     Timestamp_evenement_de_mise_en_paiement_provisoire                  STRING,
# MAGIC     Timestamp_evenement_Demande_de_paiement_definitif                   STRING,
# MAGIC     Nombre_de_points_inscrits                                           STRING,
# MAGIC     Corbeille_individuelle_derniere_tache_Constitution                  STRING,
# MAGIC     Libelle_Chemin_Arborescence_derniere_tache_Constitution             STRING,
# MAGIC     Date_limite_COM                                                     STRING,
# MAGIC     Date_limite_COM_apres_provisoire                                    STRING,
# MAGIC     Presence_dossier_IRCANTEC                                           STRING,
# MAGIC     Libelle_long_motif_couleur_Carriere                                 STRING,
# MAGIC     Libelle_long_motif_couleur_Calcul_Droits                            STRING,
# MAGIC     Libelle_long_motif_couleur_Synthese_Individu                        STRING,
# MAGIC     Libelle_long_motif_couleur_Demande_de_retraite                      STRING,
# MAGIC     Motif_colorisation_onglet_Famille                                   STRING,
# MAGIC     Nombre_de_pieces_a_traiter                                          STRING,
# MAGIC     Presence_tache_en_cours                                             STRING,
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation STRING,
# MAGIC     Libelle_EG_corbeille_Collective_d_affectation                       STRING,
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache              STRING,
# MAGIC     Complexite_carriere                                                 STRING,
# MAGIC     Nombre_de_preuves_en_attente                                        STRING,
# MAGIC     Timestamp_Envoi_Courrier_PMR                                        STRING,
# MAGIC     Identifiant_individu_de_Communication                               STRING,
# MAGIC     Flag_Stock_non_paye                                                 STRING,
# MAGIC     Flag_Stock_provisoire                                               STRING,
# MAGIC     Date_CER_Integral                                                   STRING,
# MAGIC     Code_Liquidation_Tous_Regime                                        STRING,
# MAGIC     Libelle_Statut_PSAA                                                 STRING,
# MAGIC     Corbeille_individuelle_derniere_tache_Etude                         STRING,
# MAGIC     Libelle_Chemin_Arborescence_derniere_tache_Etudes                   STRING,
# MAGIC     Statut_Tache_Dossier                                                STRING,
# MAGIC     Type_Tache_Dossier                                                  STRING,
# MAGIC     Affectation_Dossier                                                 STRING,
# MAGIC     Indicateur_Pieces_a_traiter                                         STRING,
# MAGIC     Indicateur_Absence_de_tache                                         STRING,
# MAGIC     Indicateur_NPV_recue                                                STRING,
# MAGIC     File_Name                                                           STRING,
# MAGIC     ingestion_ts                                                        STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
