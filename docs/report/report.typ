#set page(numbering: "1", margin: 2.4cm)
#set text(font: "New Computer Modern", size: 11pt)
#set par(justify: true, leading: 0.8em)
#set heading(numbering: "1.")

// ============================================================
// PAGE DE GARDE
// ============================================================

#align(center)[
  #v(4cm)
  #text(size: 22pt, weight: "bold")[IF29 — Traitement de Données]
  #v(0.5cm)
  #text(size: 18pt)[Projet : Comparaison de deux méthodes de classification]
  #text(size: 18pt)[de profils de X (ex. Twitter)]
  #v(2cm)
  #text(size: 13pt)[Rapport de projet]
  #v(1cm)
  #text(size: 11pt)[#text(fill: red)[NOMS DES MEMBRES DU GROUPE]]
  #v(0.3cm)
  #text(size: 11pt)[#text(fill: red)[DATE]]
]

#pagebreak()

// ============================================================
// TABLE DES MATIÈRES
// ============================================================

#outline(indent: 1.5em, depth: 3)

#pagebreak()

// ============================================================
// 1. INTRODUCTION
// ============================================================

= Introduction

== Contexte

Twitter (aujourd'hui X) est l'une des principales plateformes de microblogging au monde, avec plusieurs centaines de millions d'utilisateurs actifs générant un flux continu de messages courts appelés *tweets*. Cette masse de données, accessible via des APIs publiques, constitue un terrain d'analyse privilégié pour les sciences des données, qu'il s'agisse d'étudier des phénomènes sociaux, politiques ou informationnels.

Cependant, tous les profils présents sur la plateforme ne sont pas animés par des humains. Une part significative de l'activité est générée par des comptes automatisés, appelés *bots*, dont les objectifs peuvent varier de la simple diffusion d'informations (bots météorologiques, alertes sismiques) à des usages malveillants (spam, phishing, manipulation de l'opinion, désinformation). La littérature scientifique s'est emparée de cette problématique depuis le milieu des années 2010, avec des travaux fondateurs comme ceux de Ferrara et al. (2016) sur la montée des bots sociaux, de Chu et al. (2012) sur la distinction entre humain, bot et cyborg, ou encore de Varol et al. (2017) sur la détection et la caractérisation des interactions humain-bot.

== Problématique

Ce projet s'inscrit dans cette continuité. Il vise à répondre à la question suivante : *comment distinguer automatiquement un profil humain d'un profil atypique (bot, spammeur, compte automatisé) à partir des données publiques de Twitter ?*

Plus spécifiquement, nous cherchons à comparer deux paradigmes de classification :

- Une *approche supervisée* : entraîner un modèle sur un échantillon de profils préalablement labellisés manuellement (humain / bot).
- Une *approche non-supervisée* : laisser un algorithme de clustering identifier des groupes naturels dans les données, puis interpréter ces groupes *a posteriori*.

#text(
  fill: red,
)[COMPLÉTER : une ou deux phrases sur le jeu de données World Cup et son intérêt spécifique (période, événement, volume).]

== Définition d'un profil atypique

Dans le cadre de ce projet, nous définissons un *profil atypique* comme un compte dont les caractéristiques comportementales et statistiques s'écartent significativement de celles de la population moyenne des utilisateurs. Cette définition, volontairement large, recouvre plusieurs réalités :

- *Les bots* : comptes entièrement ou partiellement automatisés, caractérisés par une fréquence de publication anormalement élevée, un ratio followers/followees déséquilibré, et une faible personnalisation du profil (Ferrara et al., 2016 ; Chu et al., 2012).
- *Les spammeurs* : comptes diffusant massivement des URLs, souvent raccourcies, dans un but publicitaire ou malveillant (Benevenuto et al., 2010).
- *Les profils de désinformation* : comptes créés pour amplifier artificiellement certains messages ou simuler un soutien populaire (astroturfing).

#text(
  fill: red,
)[COMPLÉTER : précisez si vous vous concentrez uniquement sur les bots ou si vous incluez d'autres catégories. Justifiez votre choix au regard du jeu de données et des références citées.]

== Structure du rapport

Ce rapport suit le déroulement classique d'un projet de data science, en reprenant avec souplesse une approche semblable à celle du framework CRISP-DM : nous avons la compréhension métier, nous poursuivrons avec l'analyse des données, la préparation de notre base de données et la pipeline ETL, les modélisations (supervisée et non-supervisée) et enfin l'évaluation et la mise en production de nos modèles.

#pagebreak()

// ============================================================
// 2. DESCRIPTION DES DONNÉES
// ============================================================

= Description des données

== Source et format

Le jeu de données *Tweet_Worldcup.zip* est constitué de fichiers JSON contenant des tweets collectés pendant la Coupe du Monde de football 2018. Chaque ligne de chaque fichier correspond à un tweet au format JSON tel que renvoyé par l'API Twitter v1.

Un tweet typique contient :

- Les *métadonnées du tweet* : identifiant (`id`), date de création (`created_at`), texte (`full_text` ou `text`), langue (`lang`).
- Les *entités* mentionnées dans le tweet : hashtags (`#`), mentions (`@`), URLs, médias attachés.
- L'*objet utilisateur* (*user object*) imbriqué : identifiant (`user.id`), pseudonyme (`screen_name`), nom affiché (`name`), description, nombre de followers, nombre d'amis (*following*), nombre de statuts postés, statut de vérification, date
  de création du compte, etc.

Le fait que l'objet utilisateur soit *imbriqué* dans chaque tweet est une caractéristique importante du schéma de données : un même utilisateur apparaît autant de fois qu'il a de tweets dans le corpus, avec potentiellement des valeurs différentes de followers, friends, etc. selon la date du tweet (instantané du profil à l'instant de la collecte).

== Volumétrie

#text(
  fill: red,
)[INSÉRER ICI LES STATISTIQUES DE VOLUMÉTRIE : Nombre total de tweets dans la base, nombre total d'utilisateurs distincts, période couverte (premier et dernier tweet), taille de la base de données. Vous pouvez obtenir ces chiffres avec les requêtes SQL correspondantes.]

À titre indicatif, après ingestion complète, notre base de données contient environ *4,5 millions de tweets* émis par plus de *1,8 million d'utilisateurs distincts*.

== Choix du système de stockage

Le sujet du projet suggère MongoDB pour le stockage des données semi-structurées. Nous avons fait le choix alternatif de *PostgreSQL* pour les raisons suivantes : la *maturité de l'écosystème SQL* pour l'agrégation et l'analyse exploratoire (fenêtrage, agrégats, jointures) ; le support natif du type `JSONB`, qui permet de conserver les données brutes dans leur format d'origine tout en offrant des capacités d'indexation et de requêtage performantes via les expressions de chemin JSON ; et la *compatibilité avec les outils Python* de data science (psycopg2, pandas).

Ce choix constitue un compromis entre la flexibilité du NoSQL et la puissance du relationnel, particulièrement adapté à un projet mêlant données brutes (JSONB) et données structurées (tables relationnelles).

#pagebreak()

// ============================================================
// 3. PIPELINE ETL
// ============================================================

= Pipeline ETL (Extract - Transform - Load)

Cette section décrit l'architecture du pipeline de traitement des données, depuis l'ingestion des fichiers JSON bruts jusqu'à la constitution d'une table `users` exploitable pour la classification.

== Architecture générale

Le pipeline s'articule autour de deux tables PostgreSQL :

- `tweets` : stocke les tweets bruts dans leur intégralité au format JSONB. Un index d'expression est créé sur l'identifiant utilisateur extrait du JSON pour permettre des requêtes performantes par utilisateur.
- `users` : table relationnelle contenant un enregistrement par utilisateur distinct, avec le snapshot utilisateur le plus récent (dernier tweet connu).

#text(
  fill: red,
)[INSÉRER ICI UN SCHÉMA DU PIPELINE : fichiers JSON → ingest.py → tweets → extract_users.py → users. Un diagramme simple fait avec Draw.io ou Excalidraw suffit.]

== Extraction : ingestion des tweets (`ingest.py`)

Le script `src/etl/extract/ingest.py` est responsable du chargement initial des fichiers JSON dans la table `tweets`. Son fonctionnement est le suivant :

1. Il lit récursivement tous les fichiers `.json` du répertoire `data/raw/`.
2. Pour chaque fichier, chaque ligne est parsée comme un objet JSON.
3. Les champs `id`, `created_at` et `text` (ou `full_text`) sont extraits et stockés dans des colonnes SQL typées ; l'intégralité du JSON est conservée dans la colonne `raw_data` de type `JSONB`.
4. L'insertion se fait par lots (*batch*) de 2 000 tweets via `execute_values` de psycopg2, avec une clause `ON CONFLICT (id) DO NOTHING` pour garantir l'idempotence (un fichier déjà ingéré ne duplique pas les données).
5. Un fichier `processed_files.log` assure le suivi des fichiers déjà traités, permettant de reprendre l'ingestion après une interruption sans repartir de zéro.

#text(
  fill: red,
)[COMPLÉTER : indiquez le temps d'exécution total de l'ingestion ainsi que les éventuels problèmes rencontrés (lignes mal formatées, erreurs de parsing).]

La structure de la table `tweets` est la suivante :
#align(
  center,
  table(
    columns: (auto, auto, auto),
    table.header([Colonne], [Type], [Description]),
    [id], [BIGINT], [Identifiant unique du tweet (PK)],
    [created_at], [TIMESTAMP], [Date de création du tweet],
    [tweet_text], [TEXT], [Texte du tweet],
    [raw_data], [JSONB], [Objet JSON complet du tweet],
  ),
)

== Transformation : déduplication des utilisateurs (`extract_users.py`)

Le cœur de la transformation réside dans le script `src/etl/transform/extract_users.py`.

*Problème.* La table `tweets` peut contenir plusieurs occurrences du même utilisateur, chacune avec un snapshot potentiellement différent (le nombre de followers évolue dans le temps). Il est nécessaire de choisir quelle version retenir.

*Solution retenue : snapshot le plus récent.* Nous utilisons une clause `DISTINCT ON` de PostgreSQL triée par `user_id, created_at DESC`. Cela garantit que, pour chaque utilisateur, seul le tweet le plus récent est conservé — et donc la version la plus à jour de son profil *au moment de la collecte*. Cette approche est préférable à une moyenne (le nombre de followers n'est pas une mesure qu'on lisse) et à un snapshot aléatoire (non déterministe).

La requête SQL sous-jacente est :

```sql
SELECT DISTINCT ON ((raw_data->'user'->>'id')::BIGINT)
    (raw_data->'user'->>'id')::BIGINT AS user_id,
    raw_data->'user'                   AS raw_user,
    created_at                         AS tweet_created_at
FROM tweets
WHERE raw_data->'user'->>'id' IS NOT NULL
ORDER BY (raw_data->'user'->>'id')::BIGINT, created_at DESC
```

Une deuxième passe agrège par utilisateur les statistiques `tweet_count`, `first_seen_at` et `last_seen_at` (premier et dernier tweet dans notre corpus).

#text(
  fill: red,
)[COMPLÉTER : indiquez le temps d'exécution de l'extraction des utilisateurs (environ 20-25 minutes pour 4,5M de tweets). Mentionnez l'utilisation du curseur côté serveur (server-side cursor) pour éviter de charger toutes les données en mémoire.]

== Chargement : upsert vers la table `users` (`load_users.py`)

Le script `src/etl/load/load_users.py` orchestre l'ensemble du pipeline :

1. Il applique `init.sql` (création de la table `users` et des index d'expression) si ce n'est pas déjà fait.
2. Il appelle `extract_users()` pour obtenir la liste des utilisateurs uniques.
3. Il insère les utilisateurs dans la table `users` par lots de 50 000, avec une clause `ON CONFLICT (user_id) DO UPDATE` (upsert) qui garantit l'idempotence.

Un mécanisme de *checkpoint* (fichier pickle) sauvegarde la liste extraite avant l'upsert. En cas d'échec, le prochain lancement recharge le pickle et saute l'extraction, évitant ainsi de perdre 25 minutes de calcul.

#text(
  fill: red,
)[COMPLÉTER : indiquez le temps d'exécution total du chargement, le nombre d'utilisateurs chargés, et mentionnez les difficultés rencontrées (volume de données, sérialisation JSONB, transactions PostgreSQL).]

== Indexation pour la performance

Pour éviter des parcours complets de la table `tweets` à chaque requête par utilisateur, nous avons créé deux index d'expression :

```sql
CREATE INDEX idx_tweets_user_id
  ON tweets (((raw_data->'user'->>'id')::BIGINT));

CREATE INDEX idx_tweets_user_date
  ON tweets (((raw_data->'user'->>'id')::BIGINT), created_at DESC);
```

Ces index permettent à PostgreSQL d'utiliser des parcours d'index (index scan) plutôt que des parcours séquentiels (sequential scan) pour les requêtes filtrant ou triant par utilisateur. Le choix d'un index d'expression plutôt que d'une colonne générée (`GENERATED ALWAYS AS ... STORED`) est motivé par le souci de ne pas réécrire la table `tweets` qui pèse plusieurs dizaines de gigaoctets.


#pagebreak()
== Structure finale de la table `users`

#table(
  columns: (auto, auto, auto),
  table.header([*Colonne*], [*Type*], [*Description*]),
  [user_id], [BIGINT], [Identifiant Twitter du compte (PK)],
  [screen_name], [TEXT], [Pseudonyme],
  [name], [TEXT], [Nom affiché],
  [description], [TEXT], [Bio du profil],
  [location], [TEXT], [Localisation déclarée],
  [url], [TEXT], [URL dans le profil],
  [followers_count], [INT], [Nombre d'abonnés],
  [friends_count], [INT], [Nombre d'abonnements (following)],
  [listed_count], [INT], [Nombre de listes incluant ce compte],
  [favourites_count], [INT], [Nombre de likes émis],
  [statuses_count], [INT], [Nombre total de tweets postés],
  [verified], [BOOLEAN], [Compte certifié par Twitter],
  [created_at], [TIMESTAMP], [Date de création du compte],
  [profile_image_url], [TEXT], [URL de la photo de profil],
  [default_profile], [BOOLEAN], [Profil jamais personnalisé],
  [default_profile_image], [BOOLEAN], [Photo de profil par défaut],
  [raw_user], [JSONB], [Objet utilisateur complet (JSON)],
  [tweet_count], [INT], [Tweets de cet utilisateur dans le corpus],
  [last_tweet_at], [TIMESTAMP], [Date du tweet le plus récent],
  [first_seen_at], [TIMESTAMP], [Date du premier tweet capturé],
  [last_seen_at], [TIMESTAMP], [Date du dernier tweet capturé],
)

#pagebreak()

// ============================================================
// 4. LABELLISATION MANUELLE
// ============================================================

= Labellisation manuelle des profils

L'approche supervisée de classification nécessite un jeu de données d'entraînement étiqueté. En l'absence de *ground truth* (les données Twitter ne contiennent pas de label « bot » ou « humain »), nous avons constitué manuellement un corpus annoté.

== Méthodologie d'échantillonnage

Le script `src/ml/sample_users.py` extrait un échantillon aléatoire de 1 000 utilisateurs depuis la table `users`. La sélection est *déterministe et reproductible* : elle utilise un hachage MD5 combinant l'identifiant utilisateur et une graine fixe (`seed`), ce qui garantit que la même graine produit toujours le même échantillon, indépendamment de la version de PostgreSQL ou de l'état du générateur aléatoire :

```sql
SELECT user_id FROM users
ORDER BY MD5(user_id::text || 'if29')
LIMIT 1000
```

== Fichier d'annotation

Pour chaque utilisateur, le CSV exporté contient : les *métadonnées du profil* (20 colonnes issues de la table `users`) ; des *features calculées* aidant à la décision (ratio followers/friends, tweets par jour depuis la création du compte, tweets par jour dans le corpus, durée de couverture dans le corpus) ; les *5 tweets les plus récents* de l'utilisateur dans notre corpus, concaténés dans une colonne `sample_tweets` ; et deux colonnes vides à remplir : `label` (0 = humain, 1 = bot) et `notes`.

Le tableau ci-dessous détaille les indicateurs clés mis à disposition de l'annotateur.

#table(
  columns: (auto, auto, auto),
  table.header([*Colonne*], [*Signification*], [*Utilisation*]),
  [followers_count], [Nombre d'abonnés], [Un compte avec 0 followers et beaucoup de tweets est suspect],
  [friends_count], [Abonnements (following)], [Un ratio followers/friends très déséquilibré (>5) suggère un bot],
  [listed_count],
  [Listes incluant le compte],
  [Un compte jamais listé (\<2) est peu visible, potentiellement automatisé],

  [statuses_count], [Tweets totaux postés], [Un compte récent avec >10k statuts indique une automation],
  [favourites_count], [Likes émis], [Un bot like souvent mécaniquement ; un humain a un ratio likes/tweets plausible],
  [verified],
  [Compte certifié (blue check)],
  [Un compte vérifié est quasi-certainement humain (ou organisation légitime)],

  [account_created_at], [Date de création du compte], [Un compte créé récemment mais déjà très actif est suspect],
  [default_profile], [Profil jamais personnalisé], [Laissé à true = aucune customisation = indice de bot],
  [default_profile_image], [Photo par défaut (oeuf)], [Idem, absence d'avatar personnalisé],
  [followers_friends_ratio], [Ratio friends/followers], [>1 = suit plus qu'il n'est suivi ; >5 = fort soupçon de bot],
  [tweets_per_day_since_creation],
  [Tweets/jour depuis création],
  [>50 tweets/jour en moyenne est humainement impossible en continu],

  [dataset_coverage_days], [Jours de présence dans le corpus], [Permet de contextualiser le tweet_count],
  [tweets_per_day_in_dataset],
  [Tweets/jour dans le corpus],
  [Même interprétation que ci-dessus, limité à notre fenêtre d'observation],

  [sample_tweets], [5 derniers tweets], [Lecture du contenu : le texte est-il varié ? naturel ? dupliqué ?],
)

== Critères de labellisation

Nous avons adopté une grille d'analyse fondée sur la littérature scientifique, en nous appuyant notamment sur les travaux de Ferrara et al. (2016), Chu et al. (2012) et Varol et al. (2017), ainsi que sur le framework SPOT de Perez et al. (2011). Les critères sont classés en trois catégories.

=== Indicateurs de profil (User-based features)

Ces indicateurs portent sur les métadonnées du compte, indépendamment de son activité récente. Un profil atypique présente typiquement :

- Une *absence de personnalisation* : `default_profile = true` et `default_profile_image = true` simultanément signalent un compte généré automatiquement sans effort de personnalisation humaine.
- Un *déséquilibre du graphe social* : un ratio `friends / followers` supérieur à 5 indique un compte qui suit massivement sans être suivi en retour — comportement caractéristique des bots de type « follow spam ».
- Une *date de création récente* couplée à un volume de statuts anormalement élevé (ex. compte créé il y a 3 mois avec 50 000 tweets).

=== Indicateurs d'activité (Activity-based features)

Ces indicateurs sont calculés à partir de l'activité observée du compte :

- Une *fréquence de publication excessive* : un rythme soutenu de plus de 50 tweets par jour, maintenu sur une longue période, dépasse les capacités humaines. Le papier SPOT (Perez et al., 2011) qualifie ce phénomène de *degré d'agressivité*.
- Une *présence massive d'URLs* dans les tweets : les bots diffusent souvent des liens raccourcis à des fins de spam ou de phishing.

=== Indicateurs de contenu (Content-based features)

L'analyse du texte des tweets (`sample_tweets`) permet de trancher les cas ambigus :

- *Duplication de contenu* : des tweets quasi-identiques postés à intervalles réguliers indiquent une automation par template.
- *Cohérence sémantique* : un humain produit des tweets variés, contextuels, parfois avec des fautes de frappe, des émotions, des réactions à d'autres utilisateurs.
- *Présence de hashtags et mentions* : un usage excessif et systématique de `#` et `@` dans chaque tweet est un marqueur d'automation (visibilité optimisée dans le cadre SPOT).

#text(
  fill: red,
)[COMPLÉTER : décrivez le processus concret de labellisation — combien d'annotateurs, temps passé, désaccords éventuels, résolution des cas litigieux. Si vous avez mesuré l'accord inter-annotateurs (Cohen's kappa), mentionnez-le ici.]

== Statistiques de l'échantillon labellisé

#text(
  fill: red,
)[INSÉRER ICI LES STATISTIQUES APRÈS LABELLISATION : Nombre de profils labellisés « humain », nombre de profils labellisés « bot », distribution des features clés par classe, un ou deux histogrammes comparatifs (ex. followers_count par classe). INSÉRER UN GRAPHIQUE : distribution des deux classes.]

Le déséquilibre éventuel entre les deux classes (si l'une est largement majoritaire) devra être pris en compte dans le choix et l'évaluation des modèles de classification supervisée (Section 5).

#pagebreak()

// ============================================================
// 5. MÉTHODOLOGIE DE MODÉLISATION
// ============================================================

= Méthodologie de modélisation

#text(
  fill: red,
)[SECTION À COMPLÉTER APRÈS AVOIR DÉFINI LES FEATURES ET ENTRAÎNÉ LES MODÈLES. Structure suggérée ci-dessous.]

== Feature engineering

#text(
  fill: red,
)[COMPLÉTER : listez les features utilisées (user-based, activity-based, content-based) en justifiant chaque choix par une référence bibliographique. Mentionnez la normalisation (StandardScaler, MinMax) et la réduction de dimension si utilisée (PCA, t-SNE).]

== Modèle supervisé

#text(
  fill: red,
)[COMPLÉTER : choix de l'algorithme (Random Forest, SVM, Logistic Regression, XGBoost...), justification, paramètres optimaux, validation croisée, métriques d'évaluation (accuracy, precision, recall, F1-score, matrice de confusion).]

== Modèle non-supervisé

#text(
  fill: red,
)[COMPLÉTER : choix de l'algorithme (K-means, DBSCAN, clustering hiérarchique...), justification, choix du nombre de clusters (méthode du coude, silhouette), interprétation des clusters.]

== Comparaison des deux approches

#text(
  fill: red,
)[COMPLÉTER : méthodologie de comparaison. Comment évaluer le non-supervisé sans labels ? Approches possibles : ARI (Adjusted Rand Index) si on confronte les clusters aux labels manuels, analyse qualitative des clusters, etc.]

#pagebreak()

// ============================================================
// 6. RÉSULTATS
// ============================================================

= Résultats

#text(fill: red)[SECTION À COMPLÉTER APRÈS AVOIR EXÉCUTÉ LES DEUX MODÈLES.]

== Performances du modèle supervisé

#text(
  fill: red,
)[COMPLÉTER : présenter la matrice de confusion, le rapport de classification (precision/recall/f1 par classe), la courbe ROC/AUC. Commenter les features les plus importantes (feature importance pour Random Forest, coefficients pour Logistic Regression).]

== Résultats du clustering non-supervisé

#text(
  fill: red,
)[COMPLÉTER : présenter une visualisation 2D des clusters (t-SNE ou PCA), les centroïdes, la distribution des features par cluster. Interpréter chaque cluster : correspond-il à une classe identifiable ?]

== Analyse comparative

#text(
  fill: red,
)[COMPLÉTER : comparer les deux approches sur les dimensions suivantes : performance prédictive, interprétabilité, capacité à découvrir des patterns imprévus, dépendance aux labels humains, coût de mise en œuvre.]

#pagebreak()

// ============================================================
// 7. DISCUSSION, LIMITES ET PERSPECTIVES
// ============================================================

= Discussion, limites et perspectives

== Interprétation des résultats

#text(
  fill: red,
)[COMPLÉTER : que nous apprennent les résultats ? Les deux approches convergent-elles vers les mêmes profils atypiques ? Y a-t-il des profils détectés par une approche mais pas par l'autre ?]

== Limites

#text(
  fill: red,
)[COMPLÉTER : discuter les limites de l'étude. Suggestions : taille de l'échantillon labellisé (1 000 profils, peut-être insuffisant) ; biais d'annotation (subjectivité humaine, erreurs de labellisation) ; fenêtre temporelle limitée (Coupe du Monde 2018), les comportements observés sont-ils généralisables ? ; absence de features avancées (analyse de sentiment, NLP sur les tweets, score SPOT complet) ; déséquilibre des classes et impact sur les métriques.]

== Perspectives d'industrialisation

#text(
  fill: red,
)[COMPLÉTER : comment passer d'un prototype étudiant à un outil utilisable en production ? Suggestions : orchestration du pipeline avec dbt ou Airflow ; mise en place de tests de qualité des données (dbt tests, Great Expectations) ; intégration continue des mises à jour de modèle (MLOps) ; utilisation d'APIs de vérification d'URLs (VirusTotal, Google Safe Browsing) pour enrichir la dimension « danger » du score SPOT ; déploiement du modèle supervisé via une API REST (FastAPI, Flask).]

#pagebreak()

// ============================================================
// BIBLIOGRAPHIE
// ============================================================

= Bibliographie

#text(fill: red)[COMPLÉTER : mettre en forme selon le style APA ou IEEE.]

- Benevenuto, F., Magno, G., Rodrigues, T., & Almeida, V. (2010). Detecting spammers on Twitter. *Proceedings of the 7th Annual Collaboration, Electronic Messaging, Anti-Abuse and Spam Conference (CEAS)*.
- Chu, Z., Gianvecchio, S., Wang, H., & Jajodia, S. (2012). Detecting automation of Twitter accounts: Are you a human, bot, or cyborg? *IEEE Transactions on Dependable and Secure Computing*, 9(6), 811-824.
- Ferrara, E., Varol, O., Davis, C., Menczer, F., & Flammini, A. (2016). The rise of social bots. *Communications of the ACM*, 59(7), 96-104.
- Perez, C., Lemercier, M., Birregah, B., & Corpel, A. (2011). SPOT 1.0: Scoring Suspicious Profiles On Twitter. *Proceedings of the 2011 International Conference on Advances in Social Networks Analysis and Mining (ASONAM)*, 377-381.
- Varol, O., Ferrara, E., Davis, C. A., Menczer, F., & Flammini, A. (2017). Online human-bot interactions: Detection, estimation, and characterization. *Proceedings of the 11th International AAAI Conference on Web and Social Media (ICWSM)*, 280-289.

#text(fill: red)[AJOUTER D'AUTRES RÉFÉRENCES PERTINENTES UTILISÉES DANS LE RAPPORT.]
