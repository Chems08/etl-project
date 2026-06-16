-- Crée la base de métadonnées d'Airflow à côté de la base analytique du pipeline.
-- Exécuté une seule fois par Postgres au premier démarrage (/docker-entrypoint-initdb.d).
CREATE DATABASE airflow;
