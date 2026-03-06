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
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ===========================================================
# MAGIC    FABRIC LAKEHOUSE - Silve LAYER 
# MAGIC   
# MAGIC  
# MAGIC    =========================================================== */
# MAGIC 
# MAGIC /* ---------- DP_BRC_GLOBAL ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_global;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_global (
# MAGIC     Numero_Semaine                                                  INT,
# MAGIC     Code_GPS                                                        VARCHAR(10),
# MAGIC     Identifiant_technique_tache                                     INT,
# MAGIC     IRC_primo_affectation_tache                                     VARCHAR(55),
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation VARCHAR(250),
# MAGIC     Libelle_EG_corbeille_Collective_d_affectation                   VARCHAR(60),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache          VARCHAR(20),
# MAGIC     Systeme_applicatif                                              VARCHAR(60),
# MAGIC     Libelle_court_Processus_Metier                                  VARCHAR(50),
# MAGIC     Libelle_court_Sous_processus_Metier                             VARCHAR(50),
# MAGIC     Libelle_long_Etape_Metier                                       VARCHAR(100),
# MAGIC     Code_type_tache                                                 VARCHAR(11),
# MAGIC     Libelle_long_Type_Tache_Metier                                  VARCHAR(70),
# MAGIC     Timestamp_creation_tache                                        TIMESTAMP,
# MAGIC     Timestamp_Affectation_tache                                     TIMESTAMP,
# MAGIC     Libelle_long_Statut_Tache                                       VARCHAR(50),
# MAGIC     Timestamp_Statut_tache                                          TIMESTAMP,
# MAGIC     Poids_de_la_tache                                               SMALLINT,
# MAGIC     Degre_d_urgence_Tache                                           SMALLINT,
# MAGIC     Date_limite_de_traitement                                       DATE,
# MAGIC     Sensibilite                                                     VARCHAR(100),
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Distribution             VARCHAR(25),
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Reference                VARCHAR(25),
# MAGIC     Identifiant_Interne_Type_Objet_Dossier                          VARCHAR(25),
# MAGIC     Tache_en_Stock                                                  VARCHAR(3),
# MAGIC     Tache_en_entree                                                 VARCHAR(3),
# MAGIC     Tache_en_Sortie                                                 VARCHAR(3),
# MAGIC     integration_ts                                                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_BRC_CFIAB ---------- */
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dp_brc_cfiab;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_cfiab (
# MAGIC     Numero_Semaine                                                  INT,
# MAGIC     Code_GPS                                                        VARCHAR(10),
# MAGIC     Identifiant_technique_tache                                     INT,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                               VARCHAR(55),
# MAGIC     Code_type_tache                                                 VARCHAR(11),
# MAGIC     Libelle_long_type_tache                                         VARCHAR(255),
# MAGIC     Libelle_long_Statut_Tache                                       VARCHAR(50),
# MAGIC     Chemin_arborescence_nom_EG_Affectation                          VARCHAR(250),
# MAGIC     Anonymisation                                                   VARCHAR(20),
# MAGIC     Date_limite_de_traitement                                       DATE,
# MAGIC     Date_de_creation_Tache                                          TIMESTAMP,
# MAGIC     Date_statut_Tache                                               TIMESTAMP,
# MAGIC     Anciennete_Tache                                                VARCHAR(10),
# MAGIC     Libelle_information_complementaire_nombre_1                     VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_1                      INT,
# MAGIC     Libelle_information_complementaire_nombre_2                     VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_2                      VARCHAR(10),
# MAGIC     Libelle_information_complementaire_nombre_3                     VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_3                      INT,
# MAGIC     Indicateur_presence_libelle_commentaire_utilisateur              SMALLINT,
# MAGIC     Indicateur_entreprise_protegee                                  VARCHAR(1),
# MAGIC     Identifiant_interne_Type_Objet_Gestion_Reference                VARCHAR(50),
# MAGIC     Identifiant_Public_Type_Objet_CTX2                              VARCHAR(20),
# MAGIC     Tache_en_Stock                                                  VARCHAR(3),
# MAGIC     Tache_en_entree                                                 VARCHAR(3),
# MAGIC     Tache_en_Sortie                                                 VARCHAR(3),
# MAGIC     integration_ts                                                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC /* ---------- DP_BRC_DSN ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_dsn;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_dsn (
# MAGIC     Numero_Semaine                                                              INT,
# MAGIC     Code_GPS                                                                    VARCHAR(10),
# MAGIC     Identifiant_technique_tache                                                 INT,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                                           VARCHAR(55),
# MAGIC     Code_type_tache                                                             VARCHAR(11),
# MAGIC     Libelle_long_type_tache                                                     VARCHAR(255),
# MAGIC     Libelle_long_Statut_Tache                                                   VARCHAR(50),
# MAGIC     Chemin_arborescence_nom_EG_Affectation                                      VARCHAR(250),
# MAGIC     Anonymisation                                                               VARCHAR(20),
# MAGIC     Date_limite_de_traitement                                                   DATE,
# MAGIC     Date_de_creation_Tache                                                      TIMESTAMP,
# MAGIC     Date_statut_Tache                                                           TIMESTAMP,
# MAGIC     Libelle_information_1                                                       VARCHAR(50),
# MAGIC     Libelle_information_2                                                       VARCHAR(50),
# MAGIC     Libelle_information_3                                                       VARCHAR(50),
# MAGIC     Libelle_information_complementaire_nombre_1                                 VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_1                                  INT,
# MAGIC     Libelle_information_complementaire_nombre_2                                 VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_2                                  VARCHAR(12),
# MAGIC     Libelle_information_complementaire_date_1                                   VARCHAR(25),
# MAGIC     Valeur_information_complementaire_date_1                                    VARCHAR(25),
# MAGIC     Indicateur_presence_libelle_commentaire_utilisateur                         SMALLINT,
# MAGIC     SIREN                                                                       VARCHAR(20),
# MAGIC     Indicateur_Grande_entreprise                                                VARCHAR(3),
# MAGIC     Nombre_d_adhesions_ouvertes                                                 SMALLINT,
# MAGIC     Indicateur_entreprise_protegee                                              VARCHAR(1),
# MAGIC     Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice INT,
# MAGIC     Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice INT,
# MAGIC     Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice           INT,
# MAGIC     Tache_en_stock                                                              VARCHAR(3),
# MAGIC     Tache_en_entree                                                             VARCHAR(3),
# MAGIC     Tache_en_sortie                                                             VARCHAR(3),
# MAGIC     integration_ts                                                              TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_SCL_TACHE ---------- */
# MAGIC DROP TABLE IF EXISTS dp_scl_tache;
# MAGIC 
# MAGIC CREATE TABLE dp_scl_tache (
# MAGIC     Numero_Semaine                                      INT,
# MAGIC     Code_GPS                                            VARCHAR(10),
# MAGIC     Identifiant_technique_de_la_tache                   INT,
# MAGIC     GPS_CICAS                                           VARCHAR(55),
# MAGIC     Libelle_EG_corbeille_Collective                     VARCHAR(60),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache VARCHAR(20),
# MAGIC     Libelle_long_Type_Tache_Metier                      VARCHAR(255),
# MAGIC     Timestamp_creation_tache                            TIMESTAMP,
# MAGIC     Libelle_long_Statut_Tache                           VARCHAR(40),
# MAGIC     Timestamp_statut_tache                              TIMESTAMP,
# MAGIC     Poids_de_la_tache                                   SMALLINT,
# MAGIC     Date_limite_de_traitement                           DATE,
# MAGIC     Sensibilite                                         VARCHAR(30),
# MAGIC     Identifiant_du_dossier                              VARCHAR(20),
# MAGIC     Libelle_IRC_paiement                                VARCHAR(50),
# MAGIC     Libelle_court_Sous_processus_metier                 VARCHAR(50),
# MAGIC     Libelle_Type_Droit                                  VARCHAR(50),
# MAGIC     Libelle_Type_Stock_Dossier                          VARCHAR(30),
# MAGIC     Libelle_Couleur_Dossier                             VARCHAR(35),
# MAGIC     Date_RDV_Dossier                                    DATE,
# MAGIC     Date_d_effet_retenue_du_dossier                     DATE,
# MAGIC     Date_paiement_provisoire                            TIMESTAMP,
# MAGIC     Type_depot                                          VARCHAR(10),
# MAGIC     Perimetre                                           VARCHAR(15),
# MAGIC     Date_limite_COM                                     DATE,
# MAGIC     Date_limite_COM_apres_provisoire                    DATE,
# MAGIC     IRC_primo_affectation                               VARCHAR(30),
# MAGIC     BriqueSysteme_applicatif                            VARCHAR(10),
# MAGIC     integration_ts                                      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC /* ---------- DP_SCL_DOSSIER ---------- */
# MAGIC DROP TABLE IF EXISTS dp_scl_dossier;
# MAGIC 
# MAGIC CREATE TABLE dp_scl_dossier (
# MAGIC     Numero_Semaine                                                  INT,
# MAGIC     Code_GPS                                                        VARCHAR(10),
# MAGIC     Identifiant_du_dossier                                          VARCHAR(20),
# MAGIC     Libelle_GPS_paiement                                            VARCHAR(40),
# MAGIC     Libelle_IRC_paiement                                            VARCHAR(50),
# MAGIC     Date_effet_retenue                                              DATE,
# MAGIC     Libelle_Motif_Retraite                                          VARCHAR(50),
# MAGIC     Type_depot                                                      VARCHAR(15),
# MAGIC     Libelle_Canal_Origine                                           VARCHAR(50),
# MAGIC     Libelle_lieu_Residence                                           VARCHAR(30),
# MAGIC     Libelle_Type_Droit                                              VARCHAR(50),
# MAGIC     Libelle_Type_Retraite                                           VARCHAR(50),
# MAGIC     Libelle_Perimetre                                               VARCHAR(15),
# MAGIC     Derniere_Colorisation_du_dossier                                VARCHAR(30),
# MAGIC     Timestamp_reception_notification_CNAV_MSA_RSI                   TIMESTAMP,
# MAGIC     Date_reception_DLR                                              TIMESTAMP,
# MAGIC     Date_reception_preuve_etat_civil                                DATE,
# MAGIC     Date_reception_RIB                                              DATE,
# MAGIC     Timestamp_evenement_Demande_de_paiement_provisoire              TIMESTAMP,
# MAGIC     Timestamp_evenement_de_mise_en_paiement_provisoire              TIMESTAMP,
# MAGIC     Timestamp_evenement_Demande_de_paiement_definitif               TIMESTAMP,
# MAGIC     Nombre_de_points_inscrits                                       INT,
# MAGIC     Corbeille_individuelle_derniere_tache_Constitution              VARCHAR(20),
# MAGIC     Libelle_Chemin_Arborescence_derniere_tache_Constitution         VARCHAR(250),
# MAGIC     Date_limite_COM                                                 DATE,
# MAGIC     Date_limite_COM_apres_provisoire                                DATE,
# MAGIC     Presence_dossier_IRCANTEC                                       VARCHAR(15),
# MAGIC     Libelle_long_motif_couleur_Carriere                             VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Calcul_Droits                        VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Synthese_Individu                    VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Demande_de_retraite                  VARCHAR(200),
# MAGIC     Motif_colorisation_onglet_Famille                               VARCHAR(200),
# MAGIC     Nombre_de_pieces_a_traiter                                      SMALLINT,
# MAGIC     Presence_tache_en_cours                                         VARCHAR(50),
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation VARCHAR(250),
# MAGIC     Libelle_EG_corbeille_Collective_d_affectation                   VARCHAR(60),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache          VARCHAR(20),
# MAGIC     Complexite_carriere                                             VARCHAR(50),
# MAGIC     Nombre_de_preuves_en_attente                                    SMALLINT,
# MAGIC     Timestamp_Envoi_Courrier_PMR                                    TIMESTAMP,
# MAGIC     Identifiant_individu_de_Communication                           VARCHAR(15),
# MAGIC     Flag_Stock_non_paye                                             SMALLINT,
# MAGIC     Flag_Stock_provisoire                                           SMALLINT,
# MAGIC     Date_CER_Integral                                               DATE,
# MAGIC     Code_Liquidation_Tous_Regime                                    VARCHAR(3),
# MAGIC     Libelle_Statut_PSAA                                             VARCHAR(150),
# MAGIC     Corbeille_individuelle_derniere_tache_Etude                     VARCHAR(20),
# MAGIC     Libelle_Chemin_Arborescence_derniere_tache_Etudes               VARCHAR(250),
# MAGIC     Statut_Tache_Dossier                                            VARCHAR(100),
# MAGIC     Type_Tache_Dossier                                              VARCHAR(255),
# MAGIC     Affectation_Dossier                                             VARCHAR(100),
# MAGIC     Indicateur_Pieces_a_traiter                                     VARCHAR(3),
# MAGIC     Indicateur_Absence_de_tache                                     VARCHAR(3),
# MAGIC     Indicateur_NPV_recue                                            VARCHAR(3),
# MAGIC     integration_ts                                                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_BRC_CFIAB_Stock_Tache ---------- */
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dp_brc_cfiab_stock_tache;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_cfiab_stock_tache (
# MAGIC     Numero_Semaine                                      INT,
# MAGIC     Code_GPS                                            VARCHAR(10),
# MAGIC     Identifiant_technique_tache                         INT,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                   VARCHAR(55),
# MAGIC     Systeme_applicatif                                  VARCHAR(60),
# MAGIC     Libelle_court_Processus_Metier                      VARCHAR(50),
# MAGIC     Libelle_court_Sous_processus_Metier                 VARCHAR(50),
# MAGIC     Libelle_long_Etape_Metier                           VARCHAR(100),
# MAGIC     Code_type_tache                                     VARCHAR(11),
# MAGIC     Libelle_long_type_tache                             VARCHAR(255),
# MAGIC     Timestamp_creation_tache                            TIMESTAMP,
# MAGIC     Timestamp_Affectation_tache                         TIMESTAMP,
# MAGIC     Timestamp_Statut_tache                              TIMESTAMP,
# MAGIC     Libelle_long_Statut_Tache                           VARCHAR(50),
# MAGIC     Chemin_arborescence_nom_EG_Affectation              VARCHAR(250),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache VARCHAR(20),
# MAGIC     Date_limite_de_traitement                           DATE,
# MAGIC     Anciennete_Tache                                    VARCHAR(10),
# MAGIC     Libelle_information_complementaire_nombre_1         VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_1          INT,
# MAGIC     Libelle_information_complementaire_nombre_2         VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_2          VARCHAR(10),
# MAGIC     Libelle_information_complementaire_nombre_3         VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_3          INT,
# MAGIC     Indicateur_entreprise_protegee                      VARCHAR(1),
# MAGIC     Identifiant_Public_Type_Objet_CTX2                  VARCHAR(20),
# MAGIC     Identifiant_Interne_Type_Objet_Dossier              VARCHAR(25),
# MAGIC     Poids_de_la_tache                                   SMALLINT,
# MAGIC     Degre_d_urgence_Tache                               SMALLINT,
# MAGIC     Sensibilite                                         VARCHAR(100),
# MAGIC     integration_ts                                      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_BRC_DSN_Stock_Tache ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_dsn_stock_tache;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_dsn_stock_tache (
# MAGIC     Numero_Semaine                                                              INT,
# MAGIC     Code_GPS                                                                    VARCHAR(10),
# MAGIC     Identifiant_technique_tache                                                 INT,
# MAGIC     Identifiant_fonctionnel_IRC_Tache                                           VARCHAR(55),
# MAGIC     Systeme_applicatif                                                          VARCHAR(60),
# MAGIC     Libelle_court_Processus_Metier                                              VARCHAR(50),
# MAGIC     Libelle_court_Sous_processus_Metier                                         VARCHAR(50),
# MAGIC     Libelle_long_Etape_Metier                                                   VARCHAR(100),
# MAGIC     Code_type_tache                                                             VARCHAR(11),
# MAGIC     Libelle_long_type_tache                                                     VARCHAR(255),
# MAGIC     Timestamp_creation_tache                                                    TIMESTAMP,
# MAGIC     Timestamp_Affectation_tache                                                 TIMESTAMP,
# MAGIC     Timestamp_Statut_tache                                                      TIMESTAMP,
# MAGIC     Libelle_long_Statut_Tache                                                   VARCHAR(50),
# MAGIC     Chemin_arborescence_nom_EG_Affectation                                      VARCHAR(250),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache                      VARCHAR(20),
# MAGIC     Date_limite_de_traitement                                                   DATE,
# MAGIC     Libelle_information_1                                                       VARCHAR(50),
# MAGIC     Libelle_information_2                                                       VARCHAR(50),
# MAGIC     Libelle_information_3                                                       VARCHAR(50),
# MAGIC     Libelle_information_complementaire_nombre_1                                 VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_1                                  INT,
# MAGIC     Libelle_information_complementaire_nombre_2                                 VARCHAR(25),
# MAGIC     Valeur_information_complementaire_nombre_2                                  VARCHAR(12),
# MAGIC     Libelle_information_complementaire_date_1                                   VARCHAR(25),
# MAGIC     Valeur_information_complementaire_date_1                                    VARCHAR(25),
# MAGIC     SIREN                                                                       VARCHAR(20),
# MAGIC     Indicateur_Grande_entreprise                                                VARCHAR(3),
# MAGIC     Nombre_d_adhesions_ouvertes                                                 SMALLINT,
# MAGIC     Indicateur_entreprise_protegee                                              VARCHAR(1),
# MAGIC     Nombre_d_individus_moyen_d_un_identifiant_externe_entreprise_pour_un_exercice INT,
# MAGIC     Nombre_de_periodes_consolidees_en_anomalie_pour_une_entreprise_et_pour_un_exercice INT,
# MAGIC     Montant_des_cotisations_declares_de_l_entreprise_pour_un_exercice           INT,
# MAGIC     Poids_de_la_tache                                                           SMALLINT,
# MAGIC     Degre_d_urgence_Tache                                                       SMALLINT,
# MAGIC     Sensibilite                                                                 VARCHAR(100),
# MAGIC     integration_ts                                                              TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_SCL_DOSSIER_Stock_Synthese ---------- */
# MAGIC DROP TABLE IF EXISTS dp_scl_dossier_stock_synthese;
# MAGIC 
# MAGIC CREATE TABLE dp_scl_dossier_stock_synthese (
# MAGIC     Numero_Semaine                                                  INT,
# MAGIC     Code_GPS                                                        VARCHAR(10),
# MAGIC     Identifiant_du_dossier                                          VARCHAR(20),
# MAGIC     Identifiant_individu_de_Communication                           VARCHAR(15),
# MAGIC     Libelle_GPS_paiement                                            VARCHAR(40),
# MAGIC     Libelle_IRC_paiement                                            VARCHAR(50),
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation VARCHAR(250),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache          VARCHAR(20),
# MAGIC     Date_effet_retenue                                              DATE,
# MAGIC     Libelle_Motif_Retraite                                          VARCHAR(50),
# MAGIC     Type_depot                                                      VARCHAR(15),
# MAGIC     Libelle_Canal_Origine                                           VARCHAR(50),
# MAGIC     Libelle_lieu_Residence                                           VARCHAR(30),
# MAGIC     Libelle_Type_Droit                                              VARCHAR(50),
# MAGIC     Libelle_Type_Retraite                                           VARCHAR(50),
# MAGIC     Derniere_Colorisation_du_dossier                                VARCHAR(30),
# MAGIC     Libelle_long_motif_couleur_Carriere                             VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Calcul_Droits                        VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Synthese_Individu                    VARCHAR(200),
# MAGIC     Libelle_long_motif_couleur_Demande_de_retraite                  VARCHAR(200),
# MAGIC     Motif_colorisation_onglet_Famille                               VARCHAR(200),
# MAGIC     Timestamp_reception_notification_CNAV_MSA_RSI                   TIMESTAMP,
# MAGIC     Date_reception_DLR                                              TIMESTAMP,
# MAGIC     Date_reception_preuve_etat_civil                                DATE,
# MAGIC     Date_reception_RIB                                              DATE,
# MAGIC     Timestamp_evenement_Demande_de_paiement_provisoire              TIMESTAMP,
# MAGIC     Timestamp_evenement_de_mise_en_paiement_provisoire              TIMESTAMP,
# MAGIC     Timestamp_evenement_Demande_de_paiement_definitif               TIMESTAMP,
# MAGIC     Nombre_de_points_inscrits                                       INT,
# MAGIC     Presence_dossier_IRCANTEC                                       VARCHAR(15),
# MAGIC     Nombre_de_pieces_a_traiter                                      SMALLINT,
# MAGIC     Nombre_tache_en_stock                                           SMALLINT,
# MAGIC     Complexite_carriere                                             VARCHAR(50),
# MAGIC     Nombre_de_preuves_en_attente                                    SMALLINT,
# MAGIC     Flag_Stock_non_paye                                             SMALLINT,
# MAGIC     Flag_Stock_provisoire                                           SMALLINT,
# MAGIC     Indicateur_Pieces_a_traiter                                     VARCHAR(3),
# MAGIC     Indicateur_NPV_recue                                            VARCHAR(3),
# MAGIC     integration_ts                                                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /* ---------- DP_BRC_Agrege ---------- */
# MAGIC DROP TABLE IF EXISTS dp_brc_agrege;
# MAGIC 
# MAGIC CREATE TABLE dp_brc_agrege (
# MAGIC     Numero_Semaine                                                  INT,
# MAGIC     Code_GPS                                                        VARCHAR(10),
# MAGIC     IRC_primo_affectation_tache                                     VARCHAR(55),
# MAGIC     Libelle_Chemin_Arborescence_nom_de_l_entite_de_Gestion_d_affectation VARCHAR(250),
# MAGIC     Corbeille_Dernier_Identifiant_Utilisateur_RCI_rattache          VARCHAR(20),
# MAGIC     Systeme_applicatif                                              VARCHAR(60),
# MAGIC     Libelle_court_Processus_Metier                                  VARCHAR(50),
# MAGIC     Libelle_court_Sous_processus_Metier                             VARCHAR(50),
# MAGIC     Libelle_long_Etape_Metier                                       VARCHAR(100),
# MAGIC     Code_type_tache                                                 VARCHAR(11),
# MAGIC     Libelle_long_Type_Tache_Metier                                  VARCHAR(70),
# MAGIC     Libelle_long_Statut_Tache                                       VARCHAR(50),
# MAGIC     NB_TACHE_STOCK                                                  INT,
# MAGIC     NB_TACHE_ENTREE                                                 INT,
# MAGIC     NB_TACHE_SORTIE                                                 INT,
# MAGIC     integration_ts                                                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
