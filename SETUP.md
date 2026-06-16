# SETUP — Lancer le projet en local

## Prérequis

- **Docker Desktop** (ou Docker Engine + Docker Compose v2) en cours d'exécution.
- ~4 Go de RAM alloués à Docker (Airflow + Kafka + Postgres).
- Une clé API **Finnhub** gratuite : créer un compte sur <https://finnhub.io/register> et copier
  le token (`API Key`). *Optionnel si vous utilisez le mode simulé.*

## 1. Configuration

```bash
cp .env.example .env
```

Éditer `.env` :

- `FINNHUB_API_KEY=...` → coller votre clé Finnhub (pour le flux **live**).

## 2. Démarrage

```bash
docker compose up -d --build
```

Cela lance : `postgres`, `kafka`, `airflow-init` (initialise la base + l'utilisateur),
`airflow-webserver`, `airflow-scheduler`, `producer`, `consumer`, `dashboard`.

Vérifier l'état :

```bash
docker compose ps
```

> Premier lancement : Airflow met ~1–2 min à initialiser sa base. Attendez que
> `airflow-webserver` soit `healthy`/`running`.

## 3. Lancer le pipeline batch (Airflow)

1. Ouvrir <http://localhost:8080> → identifiants **admin / admin**.
2. Le DAG `stock_pipeline` est déjà actif (non *paused*).
3. Cliquer sur ▶ (**Trigger DAG**) pour lancer immédiatement l'ingestion.
4. Suivre les tâches : `ensure_schema → ingest_batch → transform`
   (toutes doivent passer au vert).

## 4. Ouvrir le dashboard

<http://localhost:8501> — les visualisations apparaissent une fois le DAG terminé.
Le flux temps réel (zone live + bougies intraday) se remplit grâce aux services `producer` et
`consumer` (déjà démarrés).

## 5. Vérifier l'idempotence

Relancer le DAG une 2ᵉ fois, puis comparer le nombre de lignes (inchangé = pas de doublon) :

```bash
docker compose exec postgres psql -U stock -d stockdb -c \
  "SELECT count(*) FROM core.stock_prices;"
```

## Commandes utiles

```bash
# Logs d'un service
docker compose logs -f producer
docker compose logs -f consumer

# Vérifier le flux live arrivé en base
docker compose exec postgres psql -U stock -d stockdb -c \
  "SELECT * FROM realtime.live_quotes ORDER BY updated_at DESC;"

# Lister les messages Kafka (debug)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stock_quotes --from-beginning --max-messages 5

# Tout arrêter
docker compose down

# Tout arrêter + supprimer les données (repartir de zéro)
docker compose down -v
```

## Dépannage

| Symptôme | Cause / solution |
|----------|------------------|
| Dashboard : « Aucune donnée batch » | Le DAG n'a pas encore tourné → le déclencher dans Airflow. |
| Zone live vide | Marché fermé → pas de cotations Finnhub ; revenir aux heures d'ouverture. |
| `producer` en erreur `FINNHUB_API_KEY manquant` | Renseigner la clé dans `.env`. |
| Airflow lent à démarrer | Normal au 1ᵉʳ run (migration DB). Attendre puis rafraîchir. |
| Port déjà utilisé (8080/8501/5432) | Adapter les ports dans `docker-compose.yml`. |
