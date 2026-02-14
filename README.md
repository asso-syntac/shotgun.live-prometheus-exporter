# Shotgun Exporter

Petit exporter Prometheus pour récupérer les chiffres d’une billetterie Shotgun (https://shotgun.live) et les afficher dans Grafana. Idéal pour suivre les ventes, les scans à l’entrée et les remboursements sans se prendre la tête.

Compte tenu des contraintes de l’API Shotgun, l’exporteur conserve un état persistant. Autant en profiter pour garder quelques informations non sensibles sur les billets, ça pourrait être utile pour la suite. Aucune donnée personnelle n’est stockée : uniquement des données ne permettant pas d’identifier l’acheteur.

## Pourquoi

- Suivre ça dans l’interface web Shotgun devient vite compliqué sur de gros événements (peu d’agrégations/historique, difficile à consolider).
- Ici, on pousse les données dans une TSDB, le requêtage est donc beaucoup plus simple et léger.
- On peut y faire converger d’autres revendeurs ou d’autres métriques au même endroit pour une vue unifiée.

## Stack de démo

- Python exporter (HTTP `/metrics`)
- vmagent → VictoriaMetrics (stockage TSDB)
- Grafana (dashboards)
- SQLite (cache local, historique tickets)

## Démarrage rapide

**Prérequis:** Docker + Docker Compose, un token API Shotgun _(un JWT à récupérer dans Settings > Integrations > Shotgun APIs)_ et votre Organizer ID.

```bash
cp .env.example .env
# Éditer .env avec vos credentials :
#   SHOTGUN_TOKEN=votre-jwt
#   SHOTGUN_ORGANIZER_ID=votre-id

mkdir -p data/{victoria-metrics,grafana}
docker compose up -d
```

- Exporter: http://localhost:9090/metrics
- API: http://localhost:9091 (voir section API ci-dessous)
- Grafana: http://localhost:3000 (admin/admin)
- VictoriaMetrics: http://localhost:8428

## Image Docker

Une image pré-construite est disponible :

```bash
docker pull ghcr.io/zalexshow/shotgun.live-prometheus-exporter:main
```

Vous pouvez remplacer `build: .` par
`image: ghcr.io/zalexshow/shotgun.live-prometheus-exporter:main` dans le
service `shotgun-exporter` si vous ne souhaitez pas construire l'image en local.

## Métriques

- `shotgun_tickets_sold_total`: tickets vendus (labels: event, ticket_title, channel)
- `shotgun_tickets_revenue_euros_total`: revenus en euros
- `shotgun_tickets_refunded_total`: tickets remboursés/annulés
- `shotgun_tickets_scanned_total`: tickets scannés
- `shotgun_events_total`: événements par statut (active/past/cancelled)

Le reste est visible sur `/metrics` une fois lancé.

## API de déclenchement manuel

L'exporter expose une API sur le port 9091 pour déclencher manuellement les différents types de scans. Utile lorsqu'un écart est détecté entre les métriques et la réalité.

**Déclencher un scan complet (tous les billets) :**
```bash
curl -X POST http://localhost:9091/trigger/full-scan
```

**Déclencher un scan incrémental (depuis le dernier billet connu) :**
```bash
curl -X POST http://localhost:9091/trigger/incremental
```

**Déclencher une mise à jour des événements :**
```bash
curl -X POST http://localhost:9091/trigger/events
```

**Vérifier l'état de l'API :**
```bash
curl http://localhost:9091/health
```

Toutes les requêtes retournent immédiatement et le scan s'exécute en arrière-plan. Les logs montrent la progression en temps réel.

## Ré-importation avec timestamps historiques

Par défaut, les métriques sont enregistrées avec le timestamp du scrape. Si vous voulez réimporter des données avec leurs timestamps d'origine (date d'achat, date de scan, etc.), utilisez le script `reimport_event.py`.

**Lister les événements disponibles :**
```bash
docker compose exec shotgun-exporter python reimport_event.py --list
```

**Tester la ré-importation (dry run) :**
```bash
docker compose exec shotgun-exporter python reimport_event.py --event 123456 --dry-run
```

**Ré-importer un événement :**
```bash
docker compose exec shotgun-exporter python reimport_event.py --event 123456
```

**Ré-importer tous les événements :**
```bash
docker compose exec shotgun-exporter python reimport_event.py --all
```

Cette opération :
1. Supprime toutes les métriques existantes pour l'événement dans VictoriaMetrics
2. Ré-insère les données avec les timestamps d'origine :
   - Ventes : `ordered_at` (date d'achat)
   - Remboursements : `ticket_canceled_at` (date d'annulation)
   - Scans : `ticket_scanned_at` (date de scan à l'entrée)

**Attention :** Cette opération supprime définitivement les données existantes. Utilisez `--dry-run` pour voir ce qui sera fait avant de l'exécuter.

## Données & persistance

- `./data/shotgun_tickets.db` (SQLite): historique et statut des tickets
- `./data/victoria-metrics/`: données time‑series
- `./data/grafana/`: dashboards/config Grafana

## Dev rapide

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python shotgun_exporter.py
```

## Crédits

C'est un mini projet porté par SYNTAC (https://syntac.fr)
Pensé pour FZL (https://www.fzlprod.com/) & Foreztival (https://foreztival.com/)
Fonctionne avec Shotgun (https://shotgun.live/)


## Notes

Exemple de structure des données conservées dans l'état/cache local (sqlite) pour chaque billet vendu _(aucune donnée personnelle)_ :

```json
{
  "order_id": 10000001,
  "currency": "eur",
  "payment_method": "card",
  "utm_source": "example.com",
  "utm_medium": "website",
  "product_id": 200100,
  "ordered_at": "2025-01-15T18:54:55.242Z",
  "event_id": 300200,
  "event_name": "Concert Exemple au Grand Théâtre",
  "event_start_time": "2025-02-10T19:00:00.000Z",
  "event_end_time": "2025-02-10T22:00:00.000Z",
  "event_cancellation_date": null,
  "event_on_sale_date": "2024-12-09T14:49:14.120Z",
  "event_creation_date": "2024-12-01T12:00:00.000Z",
  "event_publication_date": "2024-12-13T09:00:00.837Z",
  "event_launch_date": "2024-12-13T09:00:02.020Z",
  "buyer_zip_code": "00000",
  "buyer_city": "Exempleville",
  "buyer_country": "France",
  "ticket_id": 400300,
  "ticket_barcode": "12345678901234",
  "ticket_redeemed_at": null,
  "shotguner_id": 500400,
  "cancelled_at": null,
  "ticket_updated_at": "2025-01-15T18:54:55.242Z",
  "ticket_visibilities": "{public,xpress_door,promoters}",
  "ticket_price": 30,
  "ticket_title": "Catégorie A",
  "channel": "online",
  "service_fee": 5,
  "user_service_fee": 0.99,
  "producer_cost": 0,
  "vat_rate": 0.055,
  "ticket_sub_category": null,
  "ticket_status": "valid",
  "sales_status": "OnSale"
}
```
