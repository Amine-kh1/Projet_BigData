# Infrastructure - choix et commandes

## Versions

- Spark 3.5.3, Scala 2.12, Java 17, Python 3.10 (image apache/spark Ubuntu Jammy).
- Hadoop 3.3.4 (version fixee par Spark 3.5.3), connecteur hadoop-aws 3.3.4 et aws-java-sdk-bundle 1.12.262.
- Kafka 3.7.2 en KRaft combine broker/controller, 3 partitions orders, replication 1.
- MinIO sources upstream RELEASE.2025-04-22T22-12-26Z; mc sources upstream RELEASE.2025-04-16T18-13-26Z.
- JupyterLab 4.2.5, ipykernel 6.29.5, boto3 1.35.24.

## Pourquoi compiler MinIO

Le 22/11/2026, le pull de l'image officielle quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z a retourne 401 UNAUTHORIZED. Dockerfile.minio compile donc les deux tags sources officiels dans Go 1.24.2; aucun fork n'est substitue. Le build utilise go.sum present dans les sources. Un premier build telecharge et compile de nombreuses dependances; le cache Docker accelere les relances. La version affichee par ces binaires peut indiquer DEVELOPMENT.GOGET faute des metadonnees du packaging officiel; le code est bien epingle aux tags ci-dessus.

## Commandes principales

```powershell
Copy-Item .env.example .env
docker compose build jupyter minio
docker compose up -d --no-build
docker compose run --rm init-data
docker compose exec -T jupyter python3 scripts/partie1_cluster.py
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
docker compose run --rm mc ls --recursive local/lake/
```

Le dossier courant du Driver et de tous les Executors est /workspace. Tous montent le projet. PYTHONPATH inclut /workspace et /workspace/scripts. Le Driver reside dans jupyter; les Executors peuvent donc le joindre sur ce nom DNS Docker. Les applications utilisent 4 cores au maximum, 2 cores et 1g par Executor; chaque Worker annonce 2 cores et 2g. Les daemons Master/Worker/History ont SPARK_DAEMON_MEMORY=512m. Les Workers sont deux conteneurs d'une meme machine physique: cela demontre une repartition logique, pas une disponibilite multi-serveurs.

Les event logs sont ecrits sous /workspace/evidence/spark-events, partages avec Spark History Server. UI Master 8080, Workers 8081/18082, application active4040, History18081, Jupyter8888, MinIO9001 (API9000). Les ports sont publies sur127.0.0.1. MinIO et Kafka ont des volumes nommes persistants; `docker compose down` les conserve. Aucun besoin de `down -v` pour reproduire les calculs.

## Sources officielles consultees

- Spark Dockerfiles: https://github.com/apache/spark-docker/tree/master/3.5.3/scala2.12-java17-python3-ubuntu
- Spark3.5.3 POM, Hadoop3.3.4: https://github.com/apache/spark/blob/v3.5.3/pom.xml
- Spark3.5.3 monitoring: https://dist.apache.org/repos/dist/release/spark/docs/3.5.3/monitoring.html
- Kafka Docker3.7.2: https://github.com/apache/kafka/blob/3.7.2/docker/examples/README.md
- Kafka Docker guide: https://kafka.apache.org/37/getting-started/docker/
- Hadoop S3A3.3.4: https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html
- MinIO tag source: https://github.com/minio/minio/releases/tag/RELEASE.2025-04-22T22-12-26Z
- mc tag source: https://github.com/minio/mc/releases/tag/RELEASE.2025-04-16T18-13-26Z

## Verification effectuee

- `docker compose config --quiet`: code0.
- `python -m py_compile scripts/partie1_cluster.py`: code0.
- Build et execution standalone termines le 23/11/2026 : 2 Workers actifs, Kafka 3 partitions, bucket lake, Parquet Silver relu, 98 verifications metier reussies. Voir evidence/results.json et verification.json.
