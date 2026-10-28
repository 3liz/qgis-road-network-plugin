# Formation sur la gestion du Système d'Information routière RoadNetwork

* **Type de formation** : formation sur la structure et la gestion des données routières via QGIS
* **Durée de la formation** : 2 jours
* **Répartition** : 1 jour sur la présentation du modèle et des outils + 1 jour sur l'édition des données
* **Public cible** : géomaticiens, techniciens, ingénieurs, référents métiers, responsables de centre, etc.
* **Documentation** : <https://docs.3liz.org/qgis-road-network-plugin/>, dont les liens vers les chapitres sont donnés pour chaque point

## Objectifs pédagogiques

## Programme détaillé - Jour 1

### Présentation des concepts

Système d'information routier (SIR). Définitions : routes, tronçon, borne, PR+abscisse, etc.

* [Concepts : le réseau routier](../concepts/index.md#road-network)
* [Concepts : position et références](../concepts/index.md#position-and-references)
* [Concepts : référencement linéaire](../concepts/index.md#linear-referencing)
* [Glossaire](../concepts/index.md#glossary)

### Les règles de topologie

Rappels sur les graphes. Le graphe routier. Règles de topologie (nœuds/arcs).

* [Concepts : topologie](../concepts/index.md#topology)
* [Concepts : cas particuliers (discontinuités, ronds-points)](../concepts/index.md#corner-cases)
* [Édition : découpe automatique des tronçons et tronçons à ne pas découper](../user-guide/graph-editing.md#edges-which-must-not-be-cut)

### Présentation du modèle de données

Schéma PostgreSQL et tables pour stocker le graphe routier : les routes, tronçons, nœuds, bornes, etc.
Description détaillée des champs et de leur signification.

* [Guide utilisateur : le modèle de données](../user-guide/index.md#database-model)
* [Structure de la base de données (tables, champs, fonctions)](../database-guide/structure.md)
* [Description des tables et des champs](../database-guide/tables.md)
* [Champs des tronçons](../user-guide/import.md#edges-layer)
  et [des bornes](../user-guide/import.md#markers-layer) (format d'import)

### Présentation du projet QGIS

Les couches du graphe routier, l'organisation en groupes.

* [Créer le projet QGIS d'administration](../installation/index.md#create-your-database-local-interface)
* [Le projet d'administration : groupes et couches](../user-guide/qgis-project.md#administration-project)
* [Les actions des couches](../user-guide/qgis-project.md#layer-actions)

### Le plugin RoadNetwork

Installation, description des panneaux et des barres d'outil.

* [Installation de QGIS et du plugin](../installation/index.md)
* [Connexion à la base PostgreSQL](../installation/index.md#add-a-connection-to-the-postgresql-database)
* [Configurer le plugin](../installation/index.md#configure-the-plugin)
* [Créer](../installation/index.md#create-the-database-structure)
  et [mettre à jour](../installation/index.md#upgrade-database-structure)
  la structure de la base
* [Les outils d'administration et le panneau d'administration](../user-guide/index.md#administration-tools)
* [La barre d'outils, le menu et les panneaux du plugin](../user-guide/qgis-project.md#toolbar-menu-and-panels)

### Les outils de recherche et de visualisation des PR

Localiser un point avec un PR+abscisse / Cliquer ou survoler la carte pour afficher le PR+abscisse.

* [Recherche rapide d'une route ou d'un PR+abscisse](../user-guide/qgis-project.md#quick-search)
* [Afficher les références au clic ou au survol de la carte](../user-guide/qgis-project.md#get-references-on-map-click)

### Les algorithmes de traitement

Comment calculer des références à partir de géométries / créer des géométries à partir de références.

* [Les algorithmes de traitement du plugin](../user-guide/processing.md)
* [L'algorithme de mise à jour des références ou des géométries d'une couche](../user-guide/processing.md#update-managed-objects-from-the-road-graph)
* [La mise à jour automatique des objets métier lors de la fusion d'une session d'édition](../user-guide/managed-objects.md#update-rules)
* [Calculer les références à partir des géométries, et les géométries à partir des références](../user-guide/managed-objects.md#calculate-the-references)
* [Calcul automatique des références à la création ou au déplacement d'un objet](../user-guide/managed-objects.md#automatic-calculation-of-the-references)

### Les outils proposés dans Lizmap

Zoomer sur un PR+abscisse / Affichage au clic ou au survol / Édition de données en créant des géométries via les références (point/ligne).

* [Présentation des outils Lizmap](../lizmap-web-client/index.md#features)
* [Centrer la carte sur un PR+abscisse](../lizmap-web-client/index.md#center-the-map-on-references)
* [Afficher les références au clic](../lizmap-web-client/index.md#get-the-references-of-a-point)
  ou [au survol](../lizmap-web-client/index.md#show-the-references-when-hovering-the-map)
* [Créer une géométrie à partir des références](../lizmap-web-client/index.md#create-a-geometry-from-references)
* [Couper une ligne](../lizmap-web-client/index.md#split-a-line-at-a-point)
  et [fusionner deux lignes](../lizmap-web-client/index.md#merge-two-lines)
* [Installation des outils dans Lizmap](../lizmap-web-client/index.md#installation)

## Programme détaillé - Jour 2

### Importer un jeu de données de test dans une base de données

Utile pour les formations des agents, tester des fonctionnalités.

* [Créer une base de démonstration](../user-guide/import.md#create-a-demo-database)
* [Créer la structure de la base](../installation/index.md#create-the-database-structure)
* [Importer le graphe routier : tronçons et bornes](../user-guide/import.md#import-the-road-graph-edges-and-markers)
  (les fichiers modèles fournis avec le plugin contiennent des données d'exemple)
* [Lancer l'import](../user-guide/import.md#run-the-import)

### Mettre les données métiers au bon format

Ajout des colonnes nécessaires pour stocker les références. Recalcul des références. Correction des géométries.

* [Les données métier : présentation](../user-guide/managed-objects.md)
* [Préparer la table](../user-guide/managed-objects.md#prepare-the-table)
  et [l'enregistrer dans la couche des objets gérés](../user-guide/managed-objects.md#register-the-table)
* [Ajouter automatiquement les colonnes de références](../user-guide/managed-objects.md#add-the-reference-fields)
* [Calculer les références](../user-guide/managed-objects.md#calculate-the-references)
  et [les maintenir à jour](../user-guide/managed-objects.md#automatic-calculation-of-the-references)
* [Corriger les géométries en les recalculant à partir des références](../user-guide/processing.md#update-managed-objects-from-the-road-graph)
* [Recaler une trace GPS sur le graphe et obtenir ses références](../database-guide/functions.md#gps-tracks-from-a-gpx-file-to-the-road-references)

### Procédure de mise-à-jour du graphe routier : principes et étapes

Copie des données dans un bac-à-sable d'édition. Pré-visualisation des références. Validation de la session d'édition.

* [Les règles et les étapes de l'édition](../user-guide/graph-editing.md#editing-rules)
* [Les actions des sessions d'édition](../user-guide/graph-editing.md#editing-session-actions) :
  [créer](../user-guide/graph-editing.md#create-editing-session),
  [visualiser les données modifiées](../user-guide/graph-editing.md#display-edited-data),
  [supprimer](../user-guide/graph-editing.md#drop-editing-session)
* [Afficher les références au survol pendant l'édition](../user-guide/qgis-project.md#get-references-on-map-click)
* [Les tronçons à ne pas découper (ponts, échangeurs)](../user-guide/graph-editing.md#edges-which-must-not-be-cut)
* [Rappels sur l'édition dans QGIS](../user-guide/graph-editing.md#how-to-edit-the-data-in-qgis)

### Cas particulier des ronds-points

Rappel sur les caractéristiques des ronds-points. Comment créer un rond-point à partir d'une intersection existante.

* [Concepts : les ronds-points](../concepts/index.md#corner-cases)
* [Créer un rond-point](../user-guide/graph-editing.md#create-a-roundabout)

### La gestion des données métier

Lister les tables liées au graphe routier. Décider des règles de gestion. Exemple d'impacts d'une modification du graphe.

* [La couche des objets gérés et les règles de mise à jour](../user-guide/managed-objects.md#the-managed-objects-layer)
* [Les règles de mise à jour](../user-guide/managed-objects.md#update-rules)
* [Exemple d'impacts d'une modification du graphe](../user-guide/managed-objects.md#example-impacts-of-a-graph-modification)
* [Les derniers objets mis à jour](../user-guide/managed-objects.md#last-updated-objects)

### Fonctions PostgreSQL avancées

Les fonctions PostgreSQL utiles pour l'utilisation avancée du graphe routier.

* [Les fonctions utiles](../database-guide/functions.md)
* [Les fonctions de référencement linéaire, avec des exemples](../database-guide/functions.md#linear-referencing-functions)
* [Le modèle de données et ses principales fonctions](../user-guide/index.md#database-model)
* [Structure de la base de données : description de toutes les fonctions](../database-guide/structure.md)
