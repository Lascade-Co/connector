# Ads & App Analytics ETL Pipeline

This project implements ETL (Extract, Transform, Load) pipelines for Facebook Ads, Google Ads, Google Analytics 4 (GA4), and Google Play Console data, storing it in ClickHouse for analytics and visualization with Metabase.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Git

### Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd connector
   ```

2. Create required directories:
   ```bash
   mkdir -p volumes/{postgres,clickhouse,metabase}
   ```

3. Start the core services (PostgreSQL + ClickHouse):
   ```bash
   docker compose up -d
   ```

## 🏗️ Architecture

- **PostgreSQL**: Stores pipeline state and metadata
- **ClickHouse**: High-performance data warehouse for ads data
- **Metabase**: Business intelligence and data visualization (optional)

## 📊 Data Sources

### Facebook Ads
- **Schedule**: Daily at 1:10 UTC
- **Groups**: `d1`, `m4`, `d2`
- **Data**: Campaign metrics, ad performance, conversion data

### Google Ads
- **Schedule**: Daily at 3:10 UTC
- **Groups**: `d1`, `m4`, `d2`
- **Data**: Campaign performance, ad metrics, budget information

### Google Analytics 4 (GA4)
- **Schedule**: Daily at 1:30 UTC
- **Groups**: `d1` (configurable)
- **Data**: Traffic sources, user engagement, device analytics, events

### Google Play Console
- **Schedule**: Daily at 5:10 UTC 
- **Groups**: Configurable per app
- **Data**: Install statistics, crash reports, ratings, user acquisition
- **Source**: Google Cloud Storage exports

## 🛠️ Development

### Running Pipelines

#### Facebook Ads
```bash
python main.py facebook d1  # Run group d1
python main.py facebook m4  # Run group m4
python main.py facebook d2  # Run group d2
```

#### Google Ads
```bash
python main.py google d1    # Run group d1
python main.py google m4    # Run group m4
python main.py google d2    # Run group d2
```

#### Google Analytics 4 (GA4)
```bash
python main.py google_analytics d1  # Run group d1

# With backfill (120 days)
GA4_BACKFILL_DAYS=120 python main.py google_analytics d1
```

#### Google Play Console
```bash
python main.py google_play d1  # Run group d1

# With backfill (6 months)
GOOGLE_PLAY_BACKFILL_MONTHS=6 python main.py google_play d1
```

### Configuration

1. **Secrets Setup**:
   - Copy `.dlt/secrets.toml` and configure your credentials
   - Set up `facebook.json`, `google.json`, and `google_play.json` with account configurations
   - **GA4 uses the same `google.json` file as Google Ads** - just add GA4 property IDs to the `account_ids` array
   - Configure GitHub Secrets for automated workflows

2. **Pipeline Groups**:
   - Each platform has multiple groups (d1, m4, d2)
   - Groups contain different sets of ad accounts or apps
   - Configure in `facebook.json` / `google.json` / `google_play.json` / `google_analytics.json`

## 📈 Metabase Integration (Optional)

### Starting Metabase
```bash
# Start all services including Metabase
docker compose --profile metabase up -d

# Or start only Metabase
docker compose up metabase -d

# Stop Metabase
docker compose stop metabase
```

### Connecting to ClickHouse

1. Access Metabase at http://localhost:3000
2. Add ClickHouse as a data source:
   - **Host**: `clickhouse` (Docker service name)
   - **Port**: `8123` (HTTP port)
   - **Database**: `travel`
   - **Username**: `traveler`
   - **Password**: `EAAJbOELsc3wBO5Rvi9lQyZCVTI`

## 🔄 Automated Workflows

GitHub Actions automatically run the ETL pipelines:

- **Facebook ETL**: Daily at 1:10 UTC across all groups
- **GA4 ETL**: Daily at 1:30 UTC across all groups
- **Google Ads ETL**: Daily at 3:10 UTC across all groups
- **Google Play ETL**: Daily at 5:10 UTC (configure as needed)

Workflows can also be triggered manually from the GitHub Actions tab.

### Manual Backfill Workflows
- **GA4 Backfill**: `.github/workflows/ga4-backfill.yml` - Pull historical GA4 data
- **Google Ads Backfill**: `.github/workflows/google-backfill.yml` - Pull historical ads data

## 📁 Project Structure

```
connector/
├── .github/workflows/          # GitHub Actions workflows
│   ├── daily.yml              # Facebook Ads ETL (1:10 UTC)
│   ├── ga4-daily.yml          # GA4 ETL (1:30 UTC)
│   ├── ga4-backfill.yml       # GA4 manual backfill
│   ├── google-daily.yml       # Google Ads ETL (3:10 UTC)
│   ├── google-backfill.yml    # Google Ads manual backfill
│   └── google-play-daily.yml  # Google Play ETL (5:10 UTC)
├── pipelines/                 # ETL pipeline definitions
│   ├── facebook/              # Facebook Ads pipeline
│   ├── google/                # Google Ads pipeline
│   ├── google_analytics/      # GA4 pipeline
│   └── google_play/           # Google Play Console pipeline
├── google_analytics/          # GA4 dlt source (official implementation)
│   ├── GA4_SETUP.md           # GA4 setup guide
│   └── ...
├── docker-compose.yml         # Local development services
├── main.py                    # Pipeline runner
├── requirements.txt           # Python dependencies
└── GOOGLE_PLAY_SETUP.md       # Google Play setup guide
```

## 🔧 Services

### PostgreSQL (Port 5432)
- Database: `analytics`
- User: `django`

### ClickHouse (Ports 8123, 9000)
- Database: `travel`
- User: `traveler`

### Metabase (Port 3000) - Optional
- Access: http://localhost:3000
- Data directory: `./volumes/metabase/`

## 🚨 Troubleshooting

### Check Service Status
```bash
docker compose ps
```

### View Logs
```bash
# All services
docker compose logs

# Specific service
docker compose logs clickhouse
docker compose logs metabase
```

### Restart Services
```bash
# Restart all
docker compose restart

# Restart specific service
docker compose restart clickhouse
```

### Database Connections
- PostgreSQL: `localhost:5432/analytics`
- ClickHouse HTTP: `localhost:8123/travel`
- ClickHouse Native: `localhost:9000/travel`

## 🔐 Security Notes

- Database passwords are stored in environment variables
- Production deployments should use proper secret management
- Metabase should be secured with authentication in production
- Consider using Docker secrets for sensitive data
