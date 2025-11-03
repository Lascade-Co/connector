# App Store Connect Analytics Pipeline

This pipeline extracts **analytics metrics** from the App Store Connect API, including impressions, downloads, revenue, conversion rates, and user engagement data.

## ⚠️ Important: Two-Step Setup Required

The Analytics Reports API requires a **two-step process**:

1. **One-time setup (Admin key required)**: Create analytics report requests for each app
2. **Daily pipeline (Marketing/Finance key)**: Read and extract data from existing reports

**Why?** Only Admin API keys can CREATE report requests, but Marketing/Finance keys can READ existing reports.

## What Data is Extracted

### 1. App Store Engagement (`app_store_engagement`)
Marketing and acquisition metrics:
- **Impressions**: How many times your app appeared in search/browse
- **Product Page Views**: Users who viewed your app's page
- **Total Downloads**: App downloads
- **Conversion Rate**: Page views → downloads
- **App Units by Source**: Downloads broken down by traffic source

### 2. App Store Commerce (`app_store_commerce`)
Revenue and monetization metrics:
- **Total Revenue**: Proceeds from app and IAP sales
- **Paying Users**: Number of users who made purchases
- **Paying Sessions**: Sessions with purchases
- **Revenue Per Paying User**: ARPU
- **In-App Purchase Sales**: IAP-specific revenue

### 3. App Usage (`app_usage`)
User behavior and retention metrics:
- **Active Devices**: Devices with your app installed and active
- **Installations**: New installs (opt-in only)
- **Deletions**: Uninstalls
- **Sessions**: App opens and usage
- **Active Last 30 Days**: Monthly active users
- **Crashes**: App crash counts

### 4. Customer Reviews (`customer_reviews`)
Qualitative feedback:
- Star ratings
- Review text
- User feedback over time

## How It Works

### Analytics Reports API Flow

The App Store Connect Analytics API works differently from standard REST endpoints:

1. **Create Report Request** (one-time per app)
   - POST to `/v1/analyticsReportRequests` with `accessType: ONGOING`
   - This tells Apple to generate daily reports for your app

2. **List Available Reports**
   - GET `/v1/analyticsReportRequests/{id}/reports?filter[category]=APP_STORE_ENGAGEMENT`
   - Returns available report types (e.g., "App Store Impressions", "Total Downloads")

3. **Get Report Instances**
   - GET `/v1/analyticsReports/{report_id}/instances`
   - Each instance represents a time granularity (DAILY, WEEKLY, MONTHLY)

4. **Get Segment Download URLs**
   - GET `/v1/analyticsReportInstances/{instance_id}/segments`
   - Returns URLs to download the actual data

5. **Download and Parse Data**
   - Download ZIP files containing CSV/TSV data
   - Parse and load into ClickHouse

### Pipeline Architecture

```
app_store_pipeline.py
├── Resolves bundle IDs → App Store Connect UUIDs
├── Creates analytics report requests (if needed)
└── Runs analytics sources:
    ├── app_store_engagement()
    ├── app_store_commerce()
    ├── app_usage()
    └── customer_reviews()
```

## Setup Instructions

### Step 1: Create Report Requests (One-time, Admin Key Required)

**You need an Admin API key for this step only.**

1. Create an Admin API key in App Store Connect (or temporarily update your existing key to Admin role)
2. Update `secrets/app_store.json` with the Admin key credentials
3. Run the setup script:

```bash
python pipelines/app_store/create_report_requests.py d1
```

This will:
- Create ONGOING analytics report requests for all apps
- Check for existing requests (won't create duplicates)
- Take 1-2 days for Apple to generate initial data

**Output example:**
```
✓ Created ONGOING report request: abc-123-def
✓ ONGOING report request already exists: xyz-789-ghi
```

4. **Wait 1-2 days** for Apple to generate the first reports
5. (Optional) Change API key back to Marketing/Finance role for daily pipeline

### Step 2: Run Daily Pipeline (Marketing/Finance Key)

After report requests are created and data is available:

```bash
python main.py app_store d1
```

The pipeline will:
- Find existing report requests (no Admin needed)
- Download and parse analytics data
- Load into ClickHouse

## Configuration

### Secrets Format (`secrets/app_store.json`)

```json
{
  "d1": {
    "issuer_id": "your-issuer-id",
    "key_id": "your-key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "account_ids": [
      { "bundle_id": "com.yourcompany.app1", "app_name": "App 1" },
      { "bundle_id": "com.yourcompany.app2", "app_name": "App 2" }
    ]
  }
}
```

**Important**: Use `bundle_id` (e.g., `com.yourcompany.app`), not numeric Apple IDs.

### Environment Variables

- `APPSTORE_BACKFILL_DAYS`: Number of days to look back (default: 7)
- `PIPELINE_NAME_SUFFIX`: Optional suffix for pipeline name

## Running the Pipeline

```bash
# Run for group 'd1'
python main.py app_store d1

# Backfill 30 days
APPSTORE_BACKFILL_DAYS=30 python main.py app_store d1
```

## Data Schema in ClickHouse

### app_store_engagement
```sql
CREATE TABLE app_store.app_store_engagement (
    Date Date,
    App_Name String,
    Impressions UInt64,
    Product_Page_Views UInt64,
    Total_Downloads UInt64,
    Conversion_Rate Float64,
    _app_id String,
    _app_name String,
    _group_name String,
    _report_name String,
    _granularity String,
    _loaded_at DateTime
) ENGINE = MergeTree()
ORDER BY (Date, App_Name);
```

### app_store_commerce
```sql
CREATE TABLE app_store.app_store_commerce (
    Date Date,
    App_Name String,
    Total_Revenue Decimal(18,2),
    Paying_Users UInt64,
    Paying_Sessions UInt64,
    Revenue_Per_Paying_User Decimal(18,2),
    _app_id String,
    _app_name String,
    _group_name String,
    _report_name String,
    _granularity String,
    _loaded_at DateTime
) ENGINE = MergeTree()
ORDER BY (Date, App_Name);
```

### app_usage
```sql
CREATE TABLE app_store.app_usage (
    Date Date,
    App_Name String,
    Active_Devices UInt64,
    Installations UInt64,
    Deletions UInt64,
    Sessions UInt64,
    Crashes UInt64,
    _app_id String,
    _app_name String,
    _group_name String,
    _report_name String,
    _granularity String,
    _loaded_at DateTime
) ENGINE = MergeTree()
ORDER BY (Date, App_Name);
```

## Validation Queries

```sql
-- Check data loaded
SELECT 
    '_app_name' as metric,
    count(*) as rows,
    min(Date) as earliest_date,
    max(Date) as latest_date
FROM app_store.app_store_engagement
GROUP BY _app_name;

-- Daily impressions and downloads
SELECT 
    Date,
    _app_name,
    sum(Impressions) as total_impressions,
    sum(Total_Downloads) as total_downloads,
    avg(Conversion_Rate) as avg_conversion_rate
FROM app_store.app_store_engagement
GROUP BY Date, _app_name
ORDER BY Date DESC
LIMIT 30;

-- Revenue trends
SELECT 
    Date,
    _app_name,
    sum(Total_Revenue) as revenue,
    sum(Paying_Users) as paying_users
FROM app_store.app_store_commerce
GROUP BY Date, _app_name
ORDER BY Date DESC
LIMIT 30;
```

## Troubleshooting

### "No reports available yet"
- Analytics report requests take 24-48 hours to generate initial data
- Run the pipeline daily; data will appear once Apple processes the reports

### "Could not resolve App Store Connect ID"
- Verify `bundle_id` matches exactly what's in App Store Connect
- Check API key has access to those apps
- Run pipeline with logging to see accessible bundle IDs

### "403 Forbidden" on analytics endpoints
- Ensure API key has "App Manager" or "Admin" role
- Analytics access requires specific permissions in App Store Connect

### Empty data in ClickHouse
- Check that report requests were created successfully (logs will show)
- Wait 24-48 hours for initial data generation
- Verify date range with `days_back` parameter

## Best Practices

1. **Run daily**: Analytics data is generated daily by Apple
2. **Monitor report requests**: Check logs to ensure requests are created
3. **Start small**: Test with 1-2 apps first, then scale
4. **Use ongoing reports**: Don't create new report requests on every run
5. **Keep customer reviews**: Useful for sentiment analysis alongside metrics

## Differences from Old Implementation

### ❌ Removed (Metadata-focused)
- `app_info`: App metadata (name, category, age ratings)
- `app_store_versions`: Version history
- `builds`: Build information
- `in_app_purchases`: IAP metadata
- `beta_testers`: TestFlight testers

### ✅ Added (Analytics-focused)
- `app_store_engagement`: Impressions, downloads, conversion
- `app_store_commerce`: Revenue, paying users, ARPU
- `app_usage`: Active devices, installs, crashes

### ✅ Kept
- `customer_reviews`: User feedback and ratings

## References

- [App Store Connect Analytics API](https://developer.apple.com/documentation/appstoreconnectapi/analytics)
- [Downloading Analytics Reports](https://developer.apple.com/documentation/appstoreconnectapi/downloading-analytics-reports)
- [Analytics Report Categories](https://developer.apple.com/news/?id=en9v7jtv)
