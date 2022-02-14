### I. Contexte    

L’objectif du projet est de concevoir un système qui a travers le jeu de donnees GDELT permet d’analyser les evenements marquants de l’année 2021. Cette base de données a eu beaucoup d’utilisations, pour mieux comprendre l’évolution et l’impact de la crise financière du 2008 (Bayesian dynamic financial networks with time-varying predictors) ou analyser l’évolution des relations entre des pays impliquées dans des conflits (Massive Media Event Data Analysis to Assess World-Wide Political Conflict and Instability ).  

Nous allons utiliser:   
- les events (CAMEO Ontology, documentation)
- les mentions (documentation)
- le graph des conaissances ⇒ GKG, Global Knowledge Graph (documentation)  

Les fichiers du jeu de données sont indexé par deux fichiers:  
- Master CSV Data File List – English
- Master CSV Data File List – GDELT Translingual  

Pour plus d’infos consulter la documentation. Le jeu de données de GDELT v2.0 est disponible également sur Google BigQuery. Cependant vous ne devez pas l’utiliser directement pour votre projet. Vous pouvez cependant l’utiliser pour explorer la structure des données, la génération des types de données ou utiliser des données connexes (ex codes pays etc…​) .  

### II. Objectif  

L’objectif de ce projet est de proposer un système de stockage distribué, résilient et performant pour repondre aux question suivantes:  

1. afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
2. pour un pays donné en paramètre, affichez les évènements qui y ont eu place triées par le nombre de mentions (tri décroissant); permettez une agrégation par jour/mois/année
3. pour une source de donnés passée en paramètre (gkg.SourceCommonName) affichez les thèmes, personnes, lieux dont les articles de cette sources parlent ainsi que le nombre d’articles et le ton moyen des articles (pour chaque thème/personne/lieu); permettez une agrégation par jour/mois/année.
4. étudiez l’évolution des relations entre deux pays (specifies en paramètre) au cours de l’année. Vous pouvez vous baser sur la langue de l’article, le ton moyen des articles, les themes plus souvent citées, les personalités ou tout element qui vous semble pertinent.

### III. Contraintes

- Vous devez utiliser au moins 1 technologie vue en cours en expliquant les raisons de votre choix (SQL/Cassandra/MongoDB/Spark/Neo4j).
- Vous devez concevoir un système distribué et tolérant aux pannes (le système doit pouvoir continuer après la perte d’un noeud).
- Vous devez pre-charger une année de données dans votre cluster
- Vous devez utiliser le cluster openstack pour déployer votre solution.
