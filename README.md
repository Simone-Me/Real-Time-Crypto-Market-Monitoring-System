# Real-Time Crypto Market Monitoring System

Pipeline temps reel de monitoring crypto base sur:

- WebSocket Binance pour l'ingestion des trades
- Kafka comme buffer entre ingestion, processing et exposition
- Consumer analytics pour calculer les metriques live
- API REST + WebSocket pour exposer les resultats
- Dashboard HTML/CSS/JS + Chart.js pour la visualisation

## Architecture

```text
Binance WebSocket
        |
        v
Kafka topic: crypto-trades
        |
        v
Analytics consumer
  - parsing JSON
  - fenetre glissante 60 s
  - volume cumule
  - gros trades
        |
        v
Kafka topic: crypto-metrics
        |
        v
API REST + WebSocket
        |
        v
Dashboard live
```

## Pourquoi cette architecture

- Kafka decouple l'ingestion du traitement.
- Le consumer analytics peut ralentir sans bloquer la collecte.
- L'API ne lit pas directement le WebSocket exchange, elle lit une vue deja calculee.
- Le dashboard ne lit jamais Kafka directement, ce qui respecte la contrainte du projet.
- Le topic `crypto-metrics` permet d'ajouter plus tard plusieurs consommateurs sans modifier le producteur initial.

## Structure

```text
services/
  ingestion/   -> WebSocket Binance -> Kafka
  analytics/   -> Kafka trades -> calculs -> Kafka metrics
  api/         -> Kafka metrics -> REST/WebSocket -> dashboard
```

## Demarrage

### 1. Lancer Kafka

```bash
npm run infra:up
```

Kafka UI sera accessible sur `http://localhost:8080`.

### 2. Installer les dependances Node.js

```bash
npm install
```

### 3. Lancer les services

Dans trois terminaux differents:

```bash
npm run dev:ingestion
npm run dev:analytics
npm run dev:api
```

Le dashboard sera accessible sur `http://localhost:3000`.

## Variables utiles

Chaque service accepte des variables d'environnement simples:

- `KAFKA_BROKERS` par defaut `localhost:9092`
- `KAFKA_CLIENT_ID` identifiant Kafka
- `TRADE_TOPIC` par defaut `crypto-trades`
- `METRICS_TOPIC` par defaut `crypto-metrics`
- `BINANCE_STREAM_URL` par defaut `wss://stream.binance.com:9443/ws/btcusdt@trade`
- `PORT` pour l'API, par defaut `3000`
- `WINDOW_MS` fenetre glissante analytics, par defaut `60000`
- `LARGE_TRADE_USD` seuil gros trade, par defaut `50000`

## Metriques calculees

Le consumer analytics calcule:

- dernier prix
- moyenne glissante sur 60 secondes
- VWAP glissant sur 60 secondes
- volume glissant
- nombre total de trades traites
- variation de prix sur la fenetre glissante
- liste recente de gros trades

## Ordre logique recommande

### Bloc 1 - setup

1. Lancer Kafka et verifier le topic dans Kafka UI.
2. Valider qu'un producteur simple peut pousser des messages.

### Bloc 2 - ingestion

1. Connecter Binance.
2. Normaliser les trades.
3. Envoyer dans `crypto-trades`.

### Bloc 3 - processing

1. Lire `crypto-trades`.
2. Calculer les metriques en memoire.
3. Publier le snapshot dans `crypto-metrics`.

### Bloc 4 - exposition

1. L'API consomme `crypto-metrics`.
2. Elle expose `GET /api/metrics`.
3. Elle pousse la derniere vue via WebSocket.

### Bloc 5 - dashboard

1. Afficher les KPI.
2. Tracer le prix en live.
3. Afficher les gros trades et l'etat de connexion.

## Comment presenter cela a la soutenance

- Montrez d'abord le flux brut dans Kafka UI.
- Ensuite montrez que `crypto-metrics` contient des agregats, pas des trades bruts.
- Ouvrez le dashboard et expliquez que le navigateur recoit des donnees poussees par l'API via WebSocket.
- Insistez sur le decouplage: exchange -> Kafka -> analytics -> API -> navigateur.

## Suite logique

Une fois cette V1 stable, vous pourrez ajouter:

- un deuxieme exchange comme Coinbase
- Redis ou MongoDB pour persister un historique
- plusieurs symboles comme BTCUSDT et ETHUSDT
- des alertes plus avancees sur volatilite
