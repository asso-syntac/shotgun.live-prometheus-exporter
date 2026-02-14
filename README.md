# Shotgun Exporter

Exporteur Prometheus pour [Shotgun.live](https://shotgun.live) : suit les ventes de billets, scans d'entree et remboursements, et expose les metriques pour VictoriaMetrics + Grafana.

## Demarrage rapide

**Prerequis :** Docker + Docker Compose, un token API Shotgun _(JWT a recuperer dans Settings > Integrations > Shotgun APIs)_ et votre Organizer ID.

```bash
git clone https://github.com/asso-syntac/shotgun.live-prometheus-exporter.git
cd shotgun.live-prometheus-exporter

cp .env.example .env
# Editer .env avec vos credentials :
#   SHOTGUN_TOKEN=votre-jwt
#   SHOTGUN_ORGANIZER_ID=votre-id

mkdir -p data/{victoria-metrics,grafana}
docker compose up -d
```

Verifier que tout tourne :

```bash
docker compose ps
```

| Service | URL | Description |
|---------|-----|-------------|
| Exporter | http://localhost:9090/metrics | Metriques Prometheus |
| API | http://localhost:9091/health | API de declenchement |
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |
| VictoriaMetrics | http://localhost:8428 | TSDB |

> Tous les ports sont bindes sur `127.0.0.1` uniquement.

## Image Docker

L'image est publiee automatiquement par la CI :

```
ghcr.io/asso-syntac/shotgun.live-prometheus-exporter:main
```

Le `docker-compose.yml` l'utilise par defaut. Pour builder localement (dev) :

```bash
docker compose build shotgun-exporter
# ou avec un override :
# docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

## Backfill des donnees historiques

Le backfill recupere l'historique complet des billets depuis l'API Shotgun et injecte les metriques dans VictoriaMetrics avec les timestamps d'origine. Cela permet d'avoir des courbes continues dans Grafana des la mise en place.

### Comment ca marche

- Traite les evenements un par un (faible conso memoire)
- Calcule des **compteurs cumulatifs** (valeur = total courant a chaque instant)
- Remplit les trous entre les points reels (fill-forward toutes les heures) pour eviter les gaps dans Grafana
- Respecte le rate limit de l'API Shotgun (200 appels/min) avec retry automatique sur 429 et 5xx
- Progression sauvegardee en SQLite : reprise possible apres interruption

### Lancer le backfill

```bash
# Backfill complet (tous les evenements)
docker compose run --rm backfill

# Dry run (aucune donnee ecrite, utile pour verifier)
docker compose run --rm backfill python backfill_metrics.py --dry-run

# Reprendre apres interruption (saute les evenements deja traites)
docker compose run --rm backfill python backfill_metrics.py --resume
```

### Re-executer le backfill

Si vous devez relancer un backfill complet (par ex. apres avoir purge VictoriaMetrics), lancez simplement sans `--resume` : tous les evenements seront retraites.

Pour purger VictoriaMetrics avant de re-backfiller :

```bash
docker compose stop victoriametrics
rm -rf data/victoria-metrics/*
docker compose start victoriametrics
# Attendre quelques secondes que VM demarre
docker compose run --rm backfill
```

### Volume de donnees

A titre indicatif, ~12 000 billets sur 54 evenements generent environ 2M de lignes de metriques (avec fill-forward a 1h). Le backfill prend quelques minutes.

## Metriques exposees

| Metrique | Labels | Description |
|----------|--------|-------------|
| `shotgun_tickets_sold_total` | event_id, event_name, ticket_title | Billets vendus |
| `shotgun_tickets_revenue_euros_total` | event_id, event_name, ticket_title | Revenus en euros |
| `shotgun_tickets_refunded_total` | event_id, event_name, ticket_title | Billets rembourses/annules |
| `shotgun_tickets_scanned_total` | event_id, event_name | Billets scannes a l'entree |
| `shotgun_tickets_by_channel_total` | event_id, event_name, channel | Ventes par canal |
| `shotgun_tickets_by_payment_method_total` | event_id, event_name, payment_method | Ventes par moyen de paiement |
| `shotgun_tickets_by_utm_source_total` | event_id, event_name, utm_source | Ventes par source UTM |
| `shotgun_tickets_by_utm_medium_total` | event_id, event_name, utm_medium | Ventes par medium UTM |
| `shotgun_tickets_by_visibility_total` | event_id, event_name, visibility | Ventes par visibilite |
| `shotgun_tickets_fees_euros_total` | event_id, event_name, fee_type | Frais en euros |
| `shotgun_events_total` | status | Nombre d'evenements par statut |
| `shotgun_event_tickets_left` | event_id, event_name | Places restantes par evenement |

## API de declenchement manuel

L'exporter expose une API sur le port 9091 :

```bash
# Scan complet (tous les billets)
curl -X POST http://localhost:9091/trigger/full-scan

# Scan incremental (depuis le dernier billet connu)
curl -X POST http://localhost:9091/trigger/incremental

# Mise a jour des evenements
curl -X POST http://localhost:9091/trigger/events

# Health check
curl http://localhost:9091/health
```

## Configuration

Toutes les variables sont dans `.env` (voir `.env.example`) :

| Variable | Defaut | Description |
|----------|--------|-------------|
| `SHOTGUN_TOKEN` | _(obligatoire)_ | JWT API Shotgun |
| `SHOTGUN_ORGANIZER_ID` | _(obligatoire)_ | ID de l'organisateur |
| `SCRAPE_INTERVAL` | `300` | Intervalle de scrape en secondes |
| `EVENTS_FETCH_INTERVAL` | `3600` | Intervalle de refresh des evenements |
| `FULL_SCAN_INTERVAL` | `86400` | Intervalle du scan complet |
| `INCLUDE_COHOSTED_EVENTS` | `false` | Inclure les evenements co-organises |
| `SENTRY_DSN` | _(vide)_ | DSN Sentry (optionnel) |
| `GF_USER_ID` | `1000` | UID pour le conteneur Grafana |

## Donnees & persistance

Tout est dans `./data/` :

| Chemin | Contenu |
|--------|---------|
| `data/shotgun_tickets.db` | Cache SQLite (tickets, progression backfill) |
| `data/victoria-metrics/` | Donnees time-series |
| `data/grafana/` | Config et plugins Grafana |

> Le repertoire `data/` est dans `.gitignore`. Aucune donnee personnelle n'est stockee : uniquement des donnees ne permettant pas d'identifier l'acheteur.

## Architecture

```
                        +-------------------+
                        |   Shotgun API     |
                        +--------+----------+
                                 |
                    +------------+------------+
                    |                         |
           +-------v--------+    +-----------v-----------+
           | shotgun-exporter|    |    backfill (one-shot) |
           | (scrape continu)|    | (historique complet)   |
           +-------+--------+    +-----------+-----------+
                   |                         |
            /metrics                   /api/v1/import
                   |                         |
           +-------v--------+               |
           |    vmagent      +---------------+
           +-------+--------+
                   |
           +-------v--------+
           | VictoriaMetrics |
           +-------+--------+
                   |
           +-------v--------+
           |    Grafana      |
           +----------------+
```

- **vmagent** scrape l'exporter toutes les 5 min et envoie a VictoriaMetrics
- **backfill** injecte directement dans VictoriaMetrics via l'API d'import
- Les labels `job` et `instance` sont supprimes par vmagent (`victoriametrics.yml`) pour que les series backfillees et scrapees soient identiques
- VictoriaMetrics est configure avec `--search.maxStalenessInterval=2h` pour gerer les gaps entre les points de fill-forward (espaces d'1h)

## Developpement

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python shotgun_exporter.py
```

## Credits

Projet porte par [SYNTAC](https://syntac.fr), pense pour [FZL](https://www.fzlprod.com/) & [Foreztival](https://foreztival.com/).
Fonctionne avec [Shotgun](https://shotgun.live/).
