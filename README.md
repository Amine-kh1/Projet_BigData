# Plateforme Big Data - Jalon 1 (parties 1, 2 et 3)

Réalisé par **Amine Khabot** et **Aya Korchi**.

Dépôt : [Amine-kh1/Projet_BigData](https://github.com/Amine-kh1/Projet_BigData).

Ce projet met en place un cluster Spark standalone et analyse un historique de commandes avec les RDD, les DataFrames et SQL. Il comprend le code, un notebook pédagogique, les données synthétiques, un contrôle indépendant des calculs et le rapport PDF.

Le sujet annonce `producer.py` et `restaurants.json`, mais ces fichiers n'accompagnaient pas le PDF transmis. Le générateur livré est donc un **remplacement pédagogique**, identifié dans `data/manifest.json`. Les résultats concernent ces données synthétiques.

## Contenu

| Fichier / dossier | Utilité |
|---|---|
| `docker-compose.yml`, `Dockerfile`, `Dockerfile.minio` | Kafka KRaft, Master, deux Workers, MinIO, Jupyter et History Server |
| `producer.py`, `data/` | 50 000 événements déterministes et 200 restaurants |
| `scripts/partie1_cluster.py` | Vérification des deux Workers, de Spark et du bucket |
| `src/common.py`, `src/analysis.py` | Calculs RDD, DataFrame, SQL et Parquet |
| `scripts/run_batch.py` | Lancement des parties 2 et 3 |
| `notebooks/Jalon1_parties_1_3.ipynb` | Parcours commenté et lecture des résultats |
| `scripts/reference_results.py`, `scripts/verify_evidence.py` | Référence Python indépendante et rapprochement avec Spark |
| `evidence/`, `screenshots/` | Résultats, plans, lineage, stages et captures |
| `rapport/Rapport_Parties_1_2_3.pdf` | Rapport du premier jalon |

## Versions et prérequis

- Docker Desktop démarré, moteur **Linux**, Docker Compose v2 ; prévoir environ 8 Gio de mémoire disponibles pour Docker et plusieurs Gio de disque. Le premier build télécharge les dépendances et compile MinIO.
- Spark **3.5.3**, Scala **2.12**, Java **17**, Python **3.10** dans la même image pour le Driver et les Executors.
- Kafka **3.7.2**, un broker KRaft, topic `orders`, **3 partitions**, facteur de réplication 1.
- Hadoop AWS **3.3.4** et AWS SDK bundle **1.12.262**, alignés avec le Hadoop de Spark.
- MinIO : sources officielles du tag `RELEASE.2025-04-22T22-12-26Z` ; mc : tag `RELEASE.2025-04-16T18-13-26Z`. Le Dockerfile compile ces versions pour éviter les images communautaires devenues inaccessibles sur les registres testés.
- JupyterLab **4.2.5**, ipykernel **6.29.5**, boto3 **1.35.24**.

Les commandes suivantes s'exécutent dans le dossier du projet, avec PowerShell, un terminal Linux ou macOS. Elles ne nécessitent pas d'installation de Spark sur Windows.

## 1. Construire et démarrer

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose build jupyter minio
docker compose up -d --no-build
docker compose ps
```

Sous Linux/macOS, remplacer `Copy-Item .env.example .env` par `cp .env.example .env`. La copie est facultative si les valeurs par défaut conviennent. Conserver un éventuel `.env` déjà personnalisé.

Le Master, les Workers et le History Server réutilisent l'image construite pour Jupyter. L'initialisation du bucket et du topic est idempotente. Les conteneurs `kafka-init` et `minio-init` doivent terminer avec le code 0 ; leur état `Exited` est normal.

| Interface locale | Adresse | Accès de démonstration par défaut |
|---|---|---|
| Spark Master | [localhost:8080](http://localhost:8080) | Deux Workers ALIVE attendus |
| Worker 1 / Worker 2 | [8081](http://localhost:8081) / [18082](http://localhost:18082) | Interface des Workers |
| MinIO Console | [localhost:9001](http://localhost:9001) | `miniouser` / `miniosecret123` |
| MinIO API S3 | [localhost:9000](http://localhost:9000) | Point d'accès S3 |
| Jupyter | [localhost:8888](http://localhost:8888) | Jeton `bigdata-local-2026` |
| Spark UI, pendant une application | [localhost:4040](http://localhost:4040) | Le port est libéré quand le Driver s'arrête |
| Spark History Server | [localhost:18081](http://localhost:18081) | DAG conservés après l'arrêt des applications |

Ces accès sont liés à `127.0.0.1`. Les noms `spark-master`, `jupyter`, `minio` et `kafka` servent aux échanges **dans le réseau Docker**. Kafka est joignable sur `localhost:9092` depuis l'hôte et `kafka:9092` depuis les conteneurs.

## 2. Générer et déposer l'historique dans Bronze

Le jeu de données est déjà inclus. Pour le régénérer à l'identique :

```powershell
docker compose exec -T jupyter python3 producer.py --mode file --n 50000 --seed 42
docker compose run --rm init-data
```

La génération écrit exactement 50 000 lignes dans `data/orders_history.jsonl` et 200 lignes dans `data/restaurants.json`. Le second fichier est du **JSON Lines**, malgré son extension `.json`, et se lit directement avec le lecteur JSON Spark par défaut.

Le générateur produit 49 000 couples `(order_id, status)` distincts et 1 000 doublons identiques. Plusieurs états d'une même commande gardent les mêmes caractéristiques métier. Un ordre d'émission simulé introduit également des retards pour 5 % des événements uniques. Aucun `arrival_time` n'est persisté : ce fichier batch ne permet donc pas de mesurer une latence d'arrivée.

```text
lake/
  bronze/history/orders_history.jsonl
  reference/restaurants.json
  silver/orders_history/city=.../*.parquet
```

Le premier jalon ne produit pas de couche Gold ni de checkpoints de streaming.

## 3. Vérifier la partie 1

```powershell
docker compose exec -T jupyter python3 scripts/partie1_cluster.py
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
docker compose run --rm mc ls local/
```

Le script réalise une action RDD réelle, puis vérifie deux Workers actifs et la présence de `lake`. Il enregistre `evidence/partie1_cluster.json` avec la version, le master, l'appName et `sc.defaultParallelism`. Les cœurs proposés par les Workers sont vérifiés séparément : le parallélisme par défaut n'est pas un inventaire physique des cœurs.

Pour garder l'UI d'une application ouverte le temps d'une capture : ajouter `--hold-seconds 120` à la commande du script.

## 4. Exécuter les parties 2 et 3

```powershell
docker compose exec -T jupyter python3 scripts/run_batch.py
```

Ce lancement réalise toutes les opérations demandées : `sc.textFile`, parsing JSON, filtre, `map`/`reduceByKey`, panier moyen en un passage, top 5 avec `takeOrdered`, lineage, schémas explicites, CA DataFrame et SQL, jointure, CA par ville/catégorie, UDF et version native, Parquet partitionné par ville, `explain(True)` et mesures réelles.

Les trois API doivent produire exactement le même CA brut par ville. Une divergence interrompt l'exécution. Les montants RDD sont additionnés en centimes entiers et les DataFrames utilisent `DecimalType(12, 2)`.

Les KPI pédagogiques RDD/DF/SQL comptent les **événements DELIVERED bruts**, doublons compris, conformément au filtre initial du sujet. La couche Silver est ensuite dédoublonnée sur `(order_id, status)` et enrichie. Elle conserve tous les statuts valides. Des contenus contradictoires derrière une même clé métier entraînent un arrêt explicite ; aucun choix arbitraire n'est fait pour ces conflits.

L'écriture `mode("overwrite")` reconstruit **`lake/silver/orders_history/`** à chaque exécution. Elle est adaptée à ce petit historique batch reproductible. Elle ne fournit pas un mécanisme de reprise transactionnelle pour le streaming.

La règle de distance est : `court` si `< 2`, `moyen` si `2 <= distance <= 5`, `long` si `> 5`. Les comparaisons de performance utilisent une action qui consomme réellement la colonne calculée, un tour de chauffe, trois essais et un ordre alterné. Les résultats ne constituent pas un benchmark de production.

Pour utiliser d'autres chemins :

```powershell
docker compose exec -T jupyter python3 scripts/run_batch.py --orders s3a://lake/bronze/history/orders_history.jsonl --restaurants s3a://lake/reference/restaurants.json --silver s3a://lake/silver/orders_history/ --repetitions 3
```

Le notebook `notebooks/Jalon1_parties_1_3.ipynb`, accessible dans Jupyter, utilise exactement le même code. Exécuter le script et le notebook successivement : ils utilisent le même emplacement de sortie et le même port UI.

## 5. Contrôler les chiffres et les fichiers Parquet

Les contrôles Python standard ci-dessous s'exécutent depuis l'hôte. Depuis le conteneur, remplacer `python` par `docker compose exec -T jupyter python3` :

```powershell
python -m unittest discover -s tests -v
python scripts/reference_results.py
python scripts/verify_evidence.py --require-spark
docker compose run --rm mc ls --recursive local/lake/
```

La référence indépendante utilise des sommes exactes en centimes. Le vérificateur rapproche les effectifs, les montants bruts et nettoyés, les paniers moyens, le top 5 ordonné et les 25 agrégats ville/catégorie. Il échoue si les preuves Spark manquent avec `--require-spark`.

Sur la graine 42 :

| Indicateur | Historique brut | Après dédoublonnage |
|---|---:|---:|
| Événements | 50 000 | 49 000 |
| Livraisons | 14 897 | 14 626 |
| CA des livraisons (MAD) | 3 390 534,84 | 3 329 042,90 |

L'écart de CA, 61 491,94 MAD, provient des événements de livraison dupliqués. Les classements reflètent uniquement le jeu synthétique livré.

## 6. Preuves à consulter

Exécution standalone validée le **23 novembre 2026** : deux Workers actifs, bucket `lake`, 50 000 événements lus, 49 000 lignes Silver et 98 contrôles de rapprochement métier réussis. Les neuf tests Python passent également.

Temps médians observés pour le même CA brut : RDD **4,691 s**, DataFrame **3,085 s**, SQL **2,684 s**. La règle de distance prend **0,602 s** avec l'UDF et **0,502 s** avec l'expression native. Trois essais après chauffe ; ces valeurs concernent uniquement cette petite charge et cette machine.

| Preuve | Emplacement |
|---|---|
| Contrôle cluster | `evidence/partie1_cluster.json` |
| Résultats Spark, temps et contexte | `evidence/results.json` |
| Contrôle indépendant | `evidence/reference_results.json`, `evidence/verification.json` |
| Tableaux | `evidence/kpi_city_raw.csv`, `kpi_city_clean.csv`, `top5_restaurants_raw.csv`, `revenue_category_city_raw.csv` |
| Lineage | `evidence/rdd_lineage.txt` |
| Jobs et stages par action | `evidence/stage_metrics.json` |
| Jointure et UDF | `evidence/explain_join_extended.txt`, `explain_distance_udf.txt`, `explain_distance_native.txt` |
| Relecture Parquet | `evidence/silver_schema.txt`, `evidence/silver_files.json` |
| Journal d'événements Spark | `evidence/spark-events/` |
| Captures | `screenshots/` |

Dans le History Server, ouvrir `Jalon1-Orders-Batch`, puis le job correspondant à `rdd_revenue_by_city` (identifiants dans `stage_metrics.json`). Développer **DAG Visualization** et capturer le graphe ainsi que les stages. Pour la partie 3, consulter les jobs DataFrame et l'onglet SQL. Les identifiants peuvent changer à chaque relancement ; consulter ceux de l'exécution retenue.

## Arrêt et diagnostic

```powershell
docker compose logs --tail 100 jupyter spark-master spark-worker-1 spark-worker-2
docker compose logs --tail 100 minio minio-init kafka kafka-init
docker compose stop
```

`stop` conserve les conteneurs et les volumes ; `docker compose start` les redémarre. Si le moteur est introuvable, ouvrir Docker Desktop et attendre que le moteur Linux soit prêt. Si un port est déjà occupé, adapter uniquement la partie gauche de son mapping dans Compose. Le premier build MinIO peut prendre plusieurs minutes ; consulter les logs de compilation avant de conclure à un blocage.

Le bucket est conservé dans le volume `minio-data`. Les event logs et preuves sont dans le dossier partagé du projet. Les mots de passe du `.env.example` servent à cette démonstration locale.

## Périmètre et références

Le schéma du premier jalon ne comporte pas `tenant_id`. Ces agrégats ne prouvent donc pas une isolation multi-tenant. Le streaming, le watermark, Delta Lake, GraphX et GraphFrames appartiennent aux parties 4 à 6.

Les explications techniques du rapport s'appuient sur les documentations primaires : [architecture Spark](https://dlcdn.apache.org/spark/docs/3.5.3/cluster-overview.html), [RDD](https://dlcdn.apache.org/spark/docs/3.5.3/rdd-programming-guide.html), [Spark SQL et Dataset](https://dlcdn.apache.org/spark/docs/3.5.3/sql-programming-guide.html), [jointures et performances](https://dlcdn.apache.org/spark/docs/3.5.3/sql-performance-tuning.html), [EXPLAIN](https://dlcdn.apache.org/spark/docs/3.5.3/sql-ref-syntax-qry-explain.html) et [Catalyst, article des concepteurs](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

Le PDF est livré prêt à consulter. Pour le reconstruire après exécution des scripts et mise à jour des captures : `python -m pip install -r requirements-report.txt`, puis `python rapport/build_report.py`. Le script utilise Arial sous Windows et les polices Vera fournies par ReportLab ailleurs.
