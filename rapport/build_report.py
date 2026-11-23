"""Construit le rapport du jalon 1 à partir des preuves, sans chiffres inventés."""
import json
from decimal import Decimal
from pathlib import Path
from xml.sax.saxutils import escape
import reportlab

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle,
    Preformatted, Image, KeepTogether,
)
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

ROOT = Path(__file__).resolve().parents[1]
E = ROOT / 'evidence'
OUT = ROOT / 'rapport' / 'Rapport_Parties_1_2_3.pdf'
INK = colors.HexColor('#192D3B')
TEAL = colors.HexColor('#007F86')
LIGHT = colors.HexColor('#ECF5F5')
GREY = colors.HexColor('#52636E')
W = A4[0] - 100

for name, filename, fallback in [('Body', 'arial.ttf', 'Vera.ttf'), ('Bold', 'arialbd.ttf', 'VeraBd.ttf'), ('Italic', 'ariali.ttf', 'VeraIt.ttf')]:
    font = Path('C:/Windows/Fonts') / filename
    if not font.exists():
        font = Path(reportlab.__file__).parent / 'fonts' / fallback
    pdfmetrics.registerFont(TTFont(name, str(font)))
pdfmetrics.registerFontFamily('Body', normal='Body', bold='Bold', italic='Italic', boldItalic='Bold')
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='TextFR', fontName='Body', fontSize=10, leading=14.6, textColor=INK, spaceAfter=8))
styles.add(ParagraphStyle(name='SmallFR', fontName='Body', fontSize=8.1, leading=11.5, textColor=GREY, spaceAfter=5))
styles.add(ParagraphStyle(name='TitleFR', fontName='Bold', fontSize=23, leading=28, textColor=INK, spaceAfter=16))
styles.add(ParagraphStyle(name='H1FR', fontName='Bold', fontSize=19, leading=23, textColor=INK, spaceAfter=15))
styles.add(ParagraphStyle(name='H2FR', fontName='Bold', fontSize=11.5, leading=16, textColor=TEAL, spaceBefore=9, spaceAfter=7))
styles.add(ParagraphStyle(name='CellFR', fontName='Body', fontSize=8.6, leading=12, textColor=INK))
styles.add(ParagraphStyle(name='CellHeadFR', fontName='Bold', fontSize=8.5, leading=11.5, textColor=colors.white))
styles.add(ParagraphStyle(name='CodeFR', fontName='Courier', fontSize=8, leading=11, textColor=INK, backColor=LIGHT, borderPadding=9, spaceBefore=3, spaceAfter=10))
story = []

def p(text, style='TextFR'):
    story.append(Paragraph(text, styles[style]))

def h(text): p(text, 'H2FR')
def title(number, text):
    p(number.upper(), 'SmallFR')
    p(text, 'H1FR')
def code(text): story.append(Preformatted(text.strip('\n'), styles['CodeFR']))
def table(headers, rows, widths=None):
    data = [[Paragraph(escape(str(c)), styles['CellHeadFR']) for c in headers]]
    data += [[Paragraph(str(c), styles['CellFR']) for c in row] for row in rows]
    obj = Table(data, colWidths=widths, repeatRows=1, hAlign='LEFT')
    obj.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),INK), ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),9), ('RIGHTPADDING',(0,0),(-1,-1),9),
        ('TOPPADDING',(0,0),(-1,-1),7), ('BOTTOMPADDING',(0,0),(-1,-1),7),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,LIGHT]),
        ('LINEBELOW',(0,-1),(-1,-1),0.5,colors.HexColor('#BFD5D7')),
    ]))
    story.extend([obj, Spacer(1,9)])
def screenshot(name, caption, max_h=160):
    path = ROOT / 'screenshots' / name
    if path.exists():
        im = Image(str(path))
        scale = min(W / im.imageWidth, max_h / im.imageHeight)
        im.drawWidth, im.drawHeight = im.imageWidth*scale, im.imageHeight*scale
        story.append(im)
        p(caption, 'SmallFR')
    else:
        p('Capture à produire lors de la validation du cluster : ' + caption, 'SmallFR')
def money(x):
    return f'{Decimal(str(x)):,.2f}'.replace(',', ' ').replace('.', ',')
def num(x): return f'{int(x):,}'.replace(',', ' ')
def page(): story.append(PageBreak())
def ref(n, label, url):
    p(f'[{n}] <link href="{escape(url)}" color="#007F86">{escape(label)}</link>', 'SmallFR')

reference = json.loads((E/'reference_results.json').read_text(encoding='utf-8'))
manifest = json.loads((ROOT/'data/manifest.json').read_text(encoding='utf-8'))
actual = json.loads((E/'results.json').read_text(encoding='utf-8')) if (E/'results.json').exists() else None
cluster = json.loads((E/'partie1_cluster.json').read_text(encoding='utf-8')) if (E/'partie1_cluster.json').exists() else None
raw, clean = reference['raw'], reference['deduplicated']

# 1 - Une entrée en matière utile, avec provenance explicite.
p('EMSI · ÉCOSYSTÈME BIG DATA 2 · 5e ANNÉE', 'SmallFR')
story.append(Spacer(1,26))
p('Analyser un historique<br/>de commandes avec Spark', 'TitleFR')
p('Parties 1, 2 et 3 - Cluster, RDD, Spark SQL et DataFrames', 'H2FR')
p('Jalon 1 · Année universitaire 2026-2027', 'SmallFR')
p('Réalisé par <b>Amine Khabot</b> et <b>Aya Korchi</b>', 'TextFR')
p('Dépôt GitHub : <link href="https://github.com/Amine-kh1/Projet_BigData" color="#007F86">github.com/Amine-kh1/Projet_BigData</link>', 'SmallFR')
story.append(Spacer(1,22))
p('Le point de départ est simple : une plateforme de livraison veut suivre son activité sans faire porter toutes ses requêtes analytiques à sa base transactionnelle. Les événements décrivent la vie des commandes ; Spark permet de les relire, de les agréger et de préparer des données consultables par les outils de restitution.')
p('Ce premier jalon pose le socle batch. Il met en place les services, calcule le chiffre d’affaires des livraisons avec les RDD, puis reprend la même analyse avec les DataFrames et SQL. Le passage d’une API à l’autre sert aussi à vérifier que les choix techniques ne changent pas le résultat métier.')
h('Périmètre et données')
p('Le sujet fait référence à un générateur et à un référentiel qui ne sont pas inclus dans le document transmis. Le dépôt fournit donc un générateur de remplacement, déterministe, et des données entièrement synthétiques. Les noms de restaurants et les résultats présentés sont des exemples de démonstration, sans valeur commerciale réelle.')
table(['Historique de démonstration', 'Valeur'],[
    ['Événements JSON Lines',num(manifest['event_count'])],
    ['Restaurants / villes','200 / 5'],
    ['Commandes distinctes',num(manifest['order_count'])],
    ['Événements en double',f"{num(manifest['duplicate_count'])} (2 % du fichier)"],
    ['Graine / journée métier','42 / 15 novembre 2026'],
], [W*.65,W*.35])
if actual:
    deployment = 'en mode standalone, avec stockage MinIO' if actual['environment']['master'].startswith('spark://') and actual['paths']['silver'].startswith('s3a://') else 'dans le contexte indiqué avec les mesures en page 8'
    p('Validation : traitements exécutés sur Spark '+actual['environment']['spark_version']+' '+deployment+'. Les mesures proviennent des actions réellement exécutées ; un calcul Python indépendant contrôle les résultats.', 'SmallFR')
else:
    p('État de validation : données et calcul de référence contrôlés en Python. Les captures, plans physiques et mesures Spark doivent provenir d’une exécution du cluster ; ils ne sont pas remplacés par des exemples fabriqués.', 'SmallFR')
p('Repères : architecture p. 2 ; RDD p. 3-4 ; DataFrames, UDF et Parquet p. 5-6 ; plans et performances p. 7-8 ; reproduction et sources p. 9 ; preuves visuelles p. 10.', 'SmallFR')
page()

# 2 - Partie 1 intégralement sur une page.
title('Partie 1', 'Un cluster lisible et reproductible')
p('Docker Compose réunit un broker Kafka en mode KRaft, un Master Spark, deux Workers, MinIO et Jupyter. Une image Spark commune évite les différences de version Python entre le Driver et les processus de calcul. Le History Server conserve l’accès aux DAG après la fin des applications.')
table(['Composant', 'Rôle dans le déploiement'],[
    ['Driver, dans Jupyter','Exécute le programme PySpark, construit le plan et orchestre les tâches. Son adresse jupyter est accessible aux Workers.'],
    ['Cluster Manager standalone','Le Master gère les ressources disponibles. Les deux Workers hébergent les Executors des applications.'],
    ['Executors','Exécutent les tâches sur les partitions et conservent les données mises en cache. Chaque Worker propose 2 cœurs et 2 Gio au planificateur.'],
    ['SparkContext','Accessible par spark.sparkContext ; relie le Driver au cluster et permet notamment sc.textFile() et les actions RDD.'],
], [W*.29,W*.71])
p('Ces responsabilités sont distinctes : le Master alloue les ressources, tandis que le Driver planifie les jobs de son application. Un Worker n’est pas un Executor ; il peut lancer ce processus pour une application. [1]', 'SmallFR')
h('Pourquoi Spark dans ce projet ?')
p('Un enchaînement de jobs MapReduce classique multiplie les matérialisations entre étapes. Pour relire les mêmes commandes, joindre un petit référentiel et tester plusieurs agrégations, le DAG de Spark et la persistance facultative évitent une partie de ces allers-retours. Cela ne supprime pas les écritures de shuffle.')
p('La seconde limite est la latence d’un modèle centré sur le batch. Le projet doit ensuite traiter un flux Kafka : Spark offre une continuité entre l’analyse structurée batch et Structured Streaming. Ce jalon valide uniquement la première étape ; il ne mesure pas encore de latence temps réel. [2, 3]')
h('Contrôles de démarrage')
p('Les services d’initialisation créent le bucket <b>lake</b> et le topic <b>orders</b> avec <b>3 partitions</b>. Le script de la partie 1 affiche Spark, master, appName et sc.defaultParallelism. Cette dernière valeur décrit le parallélisme par défaut configuré ; elle ne constitue pas, seule, une preuve du nombre de cœurs physiques.')
if cluster:
    p('Les valeurs observées sont conservées dans evidence/partie1_cluster.json ; le contrôle du Master vérifie séparément les deux Workers actifs.', 'SmallFR')
screenshot('spark_master_workers.png','Spark Master : les deux Workers enregistrés et leurs ressources. Capture complète jointe au dépôt.',110)
page()

# 3 - RDD, code court et justifications.
title('Partie 2', 'Du JSON brut aux indicateurs par ville')
p('Le fichier contient des événements, pas une table où chaque commande n’apparaît qu’une fois. Pour suivre la consigne, l’analyse RDD commence par les lignes dont le statut vaut DELIVERED. Les états CREATED, PICKED_UP et CANCELLED sont exclus du chiffre d’affaires.')
code('''lines = sc.textFile(orders_path)
orders = lines.map(json.loads)
delivered = orders.filter(lambda r: r["status"] == "DELIVERED")

# cents() convertit le montant en entier, sans flottant binaire.
ca = delivered.map(lambda r: (r["city"], cents(r["amount_mad"])))
ca = ca.reduceByKey(lambda a, b: a + b)

stats = delivered.map(
    lambda r: (r["city"], (cents(r["amount_mad"]), 1)))
stats = stats.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1]))
results = stats.collect()''')
p('Cet extrait montre le chemin de calcul. Le script livré ajoute le contrôle des champs, le suivi des rejets et une conversion Decimal dès le parsing JSON. Les sommes RDD sont réalisées en centimes entiers ; la conversion en MAD intervient à l’affichage. On évite ainsi de comparer des sommes de flottants arrondies différemment.')
h('Un seul agrégat pour le panier moyen')
p('Chaque événement produit le couple (montant, 1). reduceByKey additionne les montants et les compteurs simultanément. Pour une ville, le panier moyen est donc somme_montants / nombre_de_livraisons ; il ne faut pas faire la moyenne de moyennes de partitions. Le résultat est arrondi à deux décimales pour sa présentation.')
code('''top5 = (delivered.map(lambda r: (r["restaurant_id"], 1))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(5, key=lambda x: (-x[1], x[0])))
print(ca.toDebugString().decode("utf-8"))''')
h('Transformations et actions')
p('<b>map, filter et reduceByKey</b> construisent des RDD sans déclencher immédiatement leur calcul : ce sont des transformations lazy. <b>collect, count et takeOrdered</b> sont des actions. toDebugString() décrit les dépendances et n’est pas une action de calcul des données. Ici, collect() ne rapatrie que les quelques lignes agrégées par ville. [2]')
page()

# 4 - Résultats exacts + DAG.
title('Partie 2', 'Résultats et lecture du DAG')
p('Les valeurs ci-dessous portent sur les événements DELIVERED bruts. Elles sont calculées sur le jeu joint au dépôt et contrôlées par une implémentation Python indépendante.')
table(['Ville','Livraisons','CA (MAD)','Panier moyen (MAD)'],[
    [r['city'],num(r['delivered_count']),money(r['revenue_mad']),money(r['average_basket_mad'])] for r in raw['cities']
], [W*.24,W*.19,W*.29,W*.28])
p(f"Le total brut atteint <b>{money(raw['total_revenue_mad'])} MAD</b> pour <b>{num(raw['delivered_count'])}</b> événements de livraison. Fès présente ici le CA le plus élevé. Ce classement décrit seulement le jeu synthétique généré, pas la demande réelle dans ces villes.")
top=raw['top5_restaurants']
h('Top 5 des restaurants par livraisons brutes')
table([r['restaurant_id'] for r in top],[[num(r['delivered_count']) for r in top]],[W/5]*5)
h('Où se situe le shuffle ?')
p('Les map et filter peuvent s’enchaîner dans un même stage. reduceByKey exige ensuite de regrouper les valeurs d’une ville : une combinaison locale réduit les volumes, puis les données sont redistribuées par clé. Cette frontière de shuffle sépare les stages. Un calcul simple de CA comporte normalement un stage de préparation et un stage de réduction ; le nombre réellement observé doit être lu pour le job concerné. [2]')
if actual:
    action=next(a for a in actual['actions'] if a['label']=='rdd_revenue_by_city')
    p(f"Observation : job(s) {', '.join(map(str,action['job_ids']))}, {action['stage_count']} stages référencés ({', '.join(map(str,action['stage_ids']))}). Le détail, les tâches et les autres actions figurent dans stage_metrics.json.", 'SmallFR')
screenshot('rdd_dag_graph.png','DAG réel du job RDD de chiffre d’affaires, depuis Spark UI. La capture complète des stages est jointe au dépôt.',165)
p('Le lineage décrit la construction des partitions. Après la perte d’un Executor, Spark peut recalculer les partitions manquantes à partir des entrées et dépendances disponibles. Cette protection suppose une source durable et ne couvre pas la perte du Driver. [2]', 'SmallFR')
page()

# 5 - Schémas et jointure.
title('Partie 3', 'Des données structurées et une jointure maîtrisée')
p('Le même historique est lu avec un StructType explicite. Ce choix fixe le contrat de lecture et évite de laisser un échantillon de données décider des types. La session travaille en UTC et event_time est un TimestampType.')
table(['Source','Types déclarés'],[
    ['Commandes','order_id, restaurant_id, customer_id, city, status, courier_id : String ; amount_mad : Decimal(12,2) ; items_count : Integer ; distance_km : Double ; event_time : Timestamp.'],
    ['Restaurants','restaurant_id, name, city, category : String ; latitude, longitude : Double.'],
    ['Contrôle de lecture','Un champ _corrupt_record recueille les lignes mal formées. Les valeurs obligatoires, montants, distances, dates et statuts sont ensuite contrôlés.'],
], [W*.23,W*.77])
code('''orders = spark.read.schema(ORDER_SCHEMA).json(orders_path)
ca_df = (valid_orders.filter(F.col("status") == "DELIVERED")
    .groupBy("city").agg(F.sum("amount_mad").alias("ca_mad")))

valid_orders.createOrReplaceTempView("orders_history")
ca_sql = spark.sql("""
  SELECT city, SUM(amount_mad) AS ca_mad
  FROM orders_history
  WHERE status = 'DELIVERED'
  GROUP BY city
""")''')
p('Le programme compare les montants exacts par ville entre RDD, DataFrame et SQL. Toute divergence interrompt l’exécution. SQL et l’API DataFrame expriment la même opération et utilisent le moteur SQL de Spark. [3]')
h('Enrichissement par restaurant')
p('La jointure est une <b>left join</b> sur restaurant_id : une commande sans correspondance reste visible pour le diagnostic. Le programme compte les références absentes et les villes contradictoires. Il vérifie aussi l’unicité des 200 clés du référentiel, car une clé dupliquée pourrait multiplier les lignes et gonfler le CA.')
code('''enriched = valid_orders.join(F.broadcast(reference),
                             "restaurant_id", "left")
ca_category = (enriched.filter(F.col("status") == "DELIVERED")
    .groupBy("city", "category")
    .agg(F.sum("amount_mad").alias("ca_mad")))''')
p('Le référentiel est volontairement diffusé par broadcast : 200 lignes représentent une dimension de petite taille. La ville du référentiel est renommée restaurant_city pour préserver le sens de la ville portée par l’événement. Les 25 combinaisons ville/catégorie sont exportées en CSV. [4]', 'SmallFR')
leaders = [max((r for r in raw['categories_by_city'] if r['city'] == city['city']), key=lambda r: Decimal(r['revenue_mad'])) for city in raw['cities']]
table(['Exemples par ville', 'Catégorie dominante', 'CA (MAD)'],[[r['city'],r['category'],money(r['revenue_mad'])] for r in leaders[:2]],[W*.29,W*.43,W*.28])
page()

# 6 - UDF et Silver.
title('Partie 3', 'Enrichir, nettoyer et conserver en Parquet')
code('''def distance_band(km):
    if km is None or km != km or km < 0:
        return None
    if km < 2:
        return "court"
    if km <= 5:
        return "moyen"
    return "long"

native = (F.when(F.col("distance_km") < 2, "court")
    .when(F.col("distance_km") <= 5, "moyen")
    .otherwise("long"))  # colonne déjà validée''')
p('Les bornes sont explicites : 2 km et 5 km appartiennent à la bande moyen. Dans la version complète, les valeurs nulles, négatives et NaN sont traitées de la même façon dans les deux variantes.')
p('L’UDF Python introduit un passage entre le moteur JVM et Python et masque sa logique interne à Catalyst. L’expression when/otherwise reste décrite dans le plan Spark. Pour cette règle simple, elle évite ce coût et permet au moteur d’optimiser davantage l’expression. Les plans exportés permettent de vérifier la présence de BatchEvalPython dans la variante UDF. Il s’agit d’un avantage attendu, à confronter aux mesures de ce déploiement. [5]')
if actual:
    d=actual['benchmark']['distance']
    table(['Mesure réelle (médiane de 3 essais)','Secondes'],[
        ['UDF Python',f"{d['python_udf']['median_seconds']:.3f}"],
        ['when / otherwise',f"{d['native_when']['median_seconds']:.3f}"],
    ], [W*.74,W*.26])
p('Le test groupe les lignes par bande et collecte les compteurs. Un simple count() après withColumn() pourrait laisser Catalyst éliminer la colonne calculée : ce serait alors une comparaison trompeuse. Les deux variantes consomment ici la même entrée en cache après matérialisation.')
h('Le nettoyage change le sens du résultat')
diff=Decimal(raw['total_revenue_mad'])-Decimal(clean['total_revenue_mad'])
p(f"La couche Silver retire les doublons sur (order_id, status). Elle conserve donc les changements d’état distincts et contient <b>{num(clean['event_count'])}</b> lignes. Le CA des livraisons uniques devient <b>{money(clean['total_revenue_mad'])} MAD</b>, soit <b>{money(diff)} MAD</b> de moins que le brut. Retirer les autres statuts avant l’écriture ferait perdre une partie de l’historique.")
code('''(silver.write.mode("overwrite").partitionBy("city")
    .parquet("s3a://lake/silver/orders_history/"))''')
p('L’écriture batch reconstruit ce seul répertoire de sortie. Les montants restent décimaux, la catégorie et distance_band sont conservées. Une relecture contrôle le nombre de lignes. Le partitionnement par ville permet ensuite d’écarter des répertoires lors d’un filtre sur city ; sur 50 000 lignes, la réduction des fichiers trop petits reste aussi à surveiller.', 'SmallFR')
p('La capture MinIO en page 10 montre les cinq partitions city. Les fichiers Parquet produits sont aussi copiés dans evidence/parquet_orders_history/ pour consultation hors du conteneur.', 'SmallFR')
page()

# 7 - lecture de explain (jamais de faux plan).
title('Partie 3', 'Lire EXPLAIN plutôt que supposer le plan')
p('Le fichier explain_join_extended.txt contient la sortie complète de explain(True). Les extraits retenus ci-dessous se lisent du bas vers le haut pour suivre les entrées, la jointure puis la projection. [6]')
table(['Niveau','Ce qu’il faut lire'],[
    ['Parsed Logical Plan','Les opérations exprimées : lecture, filtre, jointure et projection. Certaines références peuvent encore être non résolues selon l’API qui a construit le plan.'],
    ['Analyzed Logical Plan','Les colonnes sont résolues et les types sont connus, notamment le montant décimal et la clé restaurant_id.'],
    ['Optimized Logical Plan','Catalyst réécrit l’arbre : simplifications et suppression des calculs inutiles, projections ou filtres rapprochés de leur source lorsque c’est possible.'],
    ['Physical Plan','Les opérateurs qui exécuteront le calcul, la stratégie de jointure et les échanges de données.'],
], [W*.31,W*.69])
if actual:
    plan=(E/'explain_join_extended.txt').read_text(encoding='utf-8')
    lines=plan.splitlines()
    start=next(i for i,line in enumerate(lines) if '== Physical Plan ==' in line)
    selected=lines[start:start+10]
    # Un extrait sélectionné reste explicitement un extrait, sans inventer les opérateurs.
    clipped=[]
    for line in selected:
        if len(line)>92:
            clipped.append(line[:89]+'...')
        else: clipped.append(line)
    code('\n'.join(clipped))
    p('Extrait réel, lignes longues abrégées pour la mise en page. Le fichier texte joint conserve toutes les colonnes et tous les identifiants.', 'SmallFR')
    p('La stratégie relevée est <b>'+escape(actual['join']['physical_strategy'])+'</b>. Le hint broadcast est explicite dans notre code : ce résultat ne prouve donc pas que Spark aurait choisi seul cette stratégie.')
else:
    code('enriched.explain(True)\n# Sortie complète enregistrée par le script après exécution.')
h('Ce que signifie BroadcastHashJoin')
p('Le petit référentiel construit une table de hachage distribuée aux Executors via BroadcastExchange. Les partitions des commandes peuvent alors chercher leurs restaurants localement. Cette stratégie évite un repartitionnement des deux grandes entrées pour cette jointure ; elle ne supprime pas le shuffle de l’agrégation par ville et catégorie. [4]')
p('Avec un référentiel beaucoup plus volumineux, une stratégie SortMergeJoin pourrait apparaître, avec échanges et tris sur les clés. Le choix dépend du type de jointure, des statistiques, des indications fournies et des adaptations à l’exécution. Il faut vérifier le plan effectivement obtenu. [4]')
p('Le DAG DataFrame est reproduit en page 10. Avec AQE, un stage amont déjà exécuté apparaît skipped dans le job final : ce statut correspond ici à une réutilisation du résultat intermédiaire, pas à un échec.', 'SmallFR')
h('Une limite volontaire')
p('Le schéma des commandes de ces trois parties ne comporte aucun tenant_id. Il permet une analyse par ville et restaurant, mais ne démontre aucune isolation entre entreprises clientes. Ajouter une séparation multi-tenant exigerait une clé de tenant dans les événements, les jointures, les agrégats et les contrôles d’accès.')
page()

# 8 - comparaison avec mesures réelles.
title('Analyse transversale', 'Comparer les API sur une base équitable')
table(['API','Typage et optimisation','Intérêt dans ce jalon'],[
    ['RDD','En PySpark, objets Python sans contrat de colonnes imposé par Spark. Les fonctions utilisateur ne sont pas réécrites comme des expressions SQL par Catalyst.','Rendre visibles les partitions, le reduceByKey, le shuffle et le lineage.'],
    ['DataFrame / SQL','Schéma de colonnes explicite, vérifié par l’analyse du plan. Catalyst optimise les expressions relationnelles ; le moteur peut générer du code JVM et utiliser des représentations mémoire efficaces.','Décrire les agrégations, les jointures et les formats de données avec moins de code spécifique.'],
    ['Dataset[T]','API typée Scala/Java avec encoders ; contrôle de types à la compilation pour les opérations typées. Python ne fournit pas cette API. Les parties relationnelles bénéficient du moteur SQL.','Repère de comparaison théorique ; aucun temps Dataset n’est mesuré dans ce projet PySpark.'],
], [W*.20,W*.49,W*.31])
p('Catalyst agit sur le plan des requêtes. Tungsten désigne des améliorations du moteur d’exécution, notamment de gestion mémoire et de génération de code ; ce n’est pas un troisième optimiseur de requêtes. Une fonction Python arbitraire ne profite pas automatiquement des mêmes optimisations qu’une expression SQL native. [3, 5, 7]')
h('Protocole retenu')
p('La comparaison utilise le même fichier Bronze, le même filtre DELIVERED et la même somme par ville. Chaque API bénéficie d’un tour de chauffe, puis de trois essais en ordre alterné. Les actions collectent le résultat agrégé : la mesure inclut donc le travail distribué et le retour des quelques lignes au Driver. Les sources ne sont pas persistées pour ce test ; les caches système et JVM peuvent toutefois être chauds.')
if actual:
    b=actual['benchmark']['api']
    table(['API','Essais (secondes)','Médiane (s)'],[
        [api,' / '.join(f'{v:.3f}' for v in b[api]['samples_seconds']),f"{b[api]['median_seconds']:.3f}"] for api in ['RDD','DataFrame','SQL']
    ], [W*.22,W*.57,W*.21])
    env=actual['environment']
    p(f"Contexte observé : Spark {env['spark_version']}, Python {env['python_version']}, master {escape(env['master'])}, parallélisme par défaut {env['default_parallelism']}, {env['shuffle_partitions']} partitions de shuffle configurées. Les valeurs brutes et les jobs associés sont dans results.json.", 'SmallFR')
    best=min(b,key=lambda k:b[k]['median_seconds'])
    p(f"Sur cette exécution, <b>{best}</b> obtient la médiane la plus faible. Cette observation reste locale : 50 000 lignes représentent une petite charge pour un cluster. Le lancement des tâches, la sérialisation et les accès au stockage peuvent peser davantage que le seul calcul arithmétique.")
else:
    p('Les mesures sont calculées et enregistrées par le script. En l’absence d’un fichier results.json issu du cluster, aucun temps ni classement de performances n’est renseigné.')
p('Pour ce projet, les DataFrames sont un choix pratique pour la suite : le schéma, les jointures et les expressions natives correspondent directement aux besoins analytiques. Les RDD restent utiles pour comprendre et contrôler le modèle distribué ; un benchmark de petite taille ne suffit pas à les classer universellement.')
page()

# 9 - traçabilité et sources.
title('Reproduction et traçabilité', 'Retrouver chaque résultat dans le dépôt')
code('''docker compose build jupyter minio
docker compose up -d --no-build
docker compose exec jupyter python3 producer.py --mode file --n 50000
docker compose run --rm init-data
docker compose exec jupyter python3 scripts/partie1_cluster.py
docker compose exec jupyter python3 scripts/run_batch.py
python scripts/reference_results.py
python scripts/verify_evidence.py --require-spark''')
p('Le README détaille les prérequis, les accès locaux et les commandes de diagnostic. Le générateur est déterministe : conserver la graine et la version du script permet de régénérer le même historique. Le répertoire evidence rassemble les chiffres, les plans, le lineage et les journaux Spark ; screenshots contient les preuves visuelles de cette exécution.')
table(['Preuve','Fichier ou emplacement'],[
    ['Cluster et ressources','partie1_cluster.json ; capture spark_master.png ; API JSON du Master'],
    ['Calculs et rapprochements','results.json ; reference_results.json ; CSV des KPI bruts et nettoyés'],
    ['DAG, jobs et stages','stage_metrics.json ; rdd_lineage.txt ; event logs ; captures rdd_dag.png et dataframe_dag.png'],
    ['Plan et stockage','explain_join_extended.txt ; plans UDF/natif ; silver_files.json ; MinIO lake/silver/orders_history/'],
], [W*.31,W*.69])
h('Limites et prolongements')
p('Kafka est installé et son topic est contrôlé, mais ce jalon lit un fichier batch. Les retards d’émission simulés ne permettent pas de mesurer la latence d’arrivée à partir du seul event_time. Les tests de watermark, de reprise de flux et de graphes relèvent des parties suivantes. Les identifiants et les coordonnées sont synthétiques ; le contrôle d’isolation multi-tenant reste à construire avec un schéma adapté.')
h('Sources techniques')
ref(1,'Apache Spark 3.5.3 - Cluster Mode Overview','https://dlcdn.apache.org/spark/docs/3.5.3/cluster-overview.html')
ref(2,'Apache Spark 3.5.3 - RDD Programming Guide','https://dlcdn.apache.org/spark/docs/3.5.3/rdd-programming-guide.html')
ref(3,'Apache Spark 3.5.3 - Spark SQL, DataFrames and Datasets','https://dlcdn.apache.org/spark/docs/3.5.3/sql-programming-guide.html')
ref(4,'Apache Spark 3.5.3 - Performance Tuning','https://dlcdn.apache.org/spark/docs/3.5.3/sql-performance-tuning.html')
ref(5,'Armbrust et al. - Deep Dive into Spark SQL’s Catalyst Optimizer','https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html')
ref(6,'Apache Spark 3.5.3 - EXPLAIN','https://dlcdn.apache.org/spark/docs/3.5.3/sql-ref-syntax-qry-explain.html')
ref(7,'Databricks - What is Tungsten?','https://www.databricks.com/blog/what-is-tungsten')
p('Le sujet transmis constitue la source des exigences et du périmètre. Les chiffres proviennent exclusivement du jeu synthétique livré et des fichiers de preuve identifiés ci-dessus.', 'SmallFR')
page()
title('Preuves visuelles', 'Le résultat dans les interfaces')
p('Captures issues de l’exécution standalone du 23 novembre 2026. Les versions complètes des écrans et les journaux sont conservés dans le dépôt.', 'SmallFR')
screenshot('minio_silver_table.png','MinIO : lake/silver/orders_history/ contient les cinq partitions city et le marqueur _SUCCESS.',250)
screenshot('dataframe_dag_graph.png','Spark UI, job 8 : DAG du CA DataFrame. Le stage 12 est déjà calculé ; le stage 13 réalise la lecture du shuffle et l’agrégation finale. La capture dataframe_job_stages.png montre les tableaux des stages.',270)

def decorate(canvas, doc):
    canvas.saveState()
    canvas.setFillColor(TEAL)
    canvas.rect(0,A4[1]-10,A4[0],10,stroke=0,fill=1)
    canvas.setFont('Body',8)
    canvas.setFillColor(GREY)
    canvas.drawString(50,A4[1]-33,'ÉCOSYSTÈME BIG DATA 2  /  JALON 1')
    canvas.setStrokeColor(colors.HexColor('#D9E3E7'))
    canvas.line(50,43,A4[0]-50,43)
    canvas.drawString(50,29,'PARTIES 1 À 3  ·  SPARK 3.5')
    canvas.drawRightString(A4[0]-50,29,str(doc.page))
    canvas.restoreState()

doc=SimpleDocTemplate(str(OUT),pagesize=A4,rightMargin=50,leftMargin=50,topMargin=57,bottomMargin=57,
    title='Rapport - Parties 1, 2 et 3 - Plateforme Big Data',author='Amine Khabot et Aya Korchi')
doc.build(story,onFirstPage=decorate,onLaterPages=decorate)
print(OUT)
