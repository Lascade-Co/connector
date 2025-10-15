# App Store Connect Quick Start Checklist

Use this checklist to quickly set up the App Store Connect ETL pipeline.

## ✅ Prerequisites

- [ ] Apple Developer Program membership ($99/year)
- [ ] Admin or App Manager access to App Store Connect
- [ ] Python 3.9+ installed
- [ ] ClickHouse database running

## ✅ Step 1: Get API Credentials

- [ ] Log in to [App Store Connect](https://appstoreconnect.apple.com/)
- [ ] Go to **Users and Access** → **Keys** tab
- [ ] Click **+** to generate new API key
- [ ] Name: "ETL Pipeline Key"
- [ ] Access level: **Admin** or **App Manager**
- [ ] Click **Generate**
- [ ] **Download** the `.p8` private key file (you can only do this once!)
- [ ] Note down:
  - [ ] **Issuer ID** (from top of Keys page)
  - [ ] **Key ID** (10-character string)

## ✅ Step 2: Secure Your Private Key

```bash
# Create secure directory
mkdir -p ~/.app-store-keys
chmod 700 ~/.app-store-keys

# Move the key
mv ~/Downloads/AuthKey_*.p8 ~/.app-store-keys/
chmod 600 ~/.app-store-keys/AuthKey_*.p8
```

- [ ] Private key stored securely
- [ ] File permissions set to 600

## ✅ Step 3: Get Your App IDs

- [ ] Go to [App Store Connect](https://appstoreconnect.apple.com/) → **My Apps**
- [ ] For each app, note the **Apple ID** (numeric, e.g., 1234567890)

## ✅ Step 4: Configure the Pipeline

```bash
# Copy example config
cp app_store.json.example app_store.json
```

- [ ] Edit `app_store.json` with your credentials:
  - [ ] `issuer_id`: Your Issuer ID
  - [ ] `key_id`: Your Key ID
  - [ ] `private_key_path`: Full path to your `.p8` file
  - [ ] `apps`: Array with your app IDs and names

- [ ] Add `app_store.json` to `.gitignore` (should already be there)

## ✅ Step 5: Install Dependencies

```bash
pip install -r requirements.txt
```

- [ ] All dependencies installed successfully

## ✅ Step 6: Test the Pipeline

```bash
# Test run
python main.py app_store d1
```

- [ ] Pipeline runs without errors
- [ ] Check ClickHouse for data:

```sql
SHOW TABLES FROM app_store;
SELECT * FROM app_store.apps LIMIT 5;
```

- [ ] Data appears in ClickHouse

## ✅ Step 7: Set Up GitHub Actions (Optional)

### For Automated Daily Runs:

- [ ] Go to GitHub repo → **Settings** → **Secrets and variables** → **Actions**
- [ ] Create secret `APP_STORE_CONFIG`:
  - [ ] Value: Entire contents of `app_store.json`
  - [ ] **Important**: Use inline private key (not file path) for GitHub Actions

- [ ] Workflows are ready:
  - [ ] `.github/workflows/app-store-daily.yml` (daily at 6:10 UTC)
  - [ ] `.github/workflows/app-store-backfill.yml` (manual backfill)

## ✅ Verification

- [ ] Pipeline runs successfully locally
- [ ] Data loads into ClickHouse `app_store` database
- [ ] All configured apps appear in `app_store.apps` table
- [ ] Reviews, versions, and builds are being extracted
- [ ] GitHub Actions workflow (if configured) runs successfully

## 📊 Available Data Tables

After successful run, you should see these tables in ClickHouse:

- [ ] `app_store.apps` - App metadata
- [ ] `app_store.app_info` - Detailed app information
- [ ] `app_store.customer_reviews` - User reviews
- [ ] `app_store.app_store_versions` - Version history
- [ ] `app_store.builds` - Build information
- [ ] `app_store.in_app_purchases` - IAP products
- [ ] `app_store.beta_testers` - TestFlight testers

## 🚨 Troubleshooting

If you encounter issues:

1. **401 Unauthorized**:
   - [ ] Verify `issuer_id` and `key_id` are correct
   - [ ] Check private key file path
   - [ ] Ensure API key has correct permissions

2. **404 Not Found**:
   - [ ] Verify app IDs are correct
   - [ ] Ensure API key has access to the apps

3. **Private Key Errors**:
   - [ ] Check private key includes `-----BEGIN PRIVATE KEY-----` header
   - [ ] Verify file is readable: `cat ~/.app-store-keys/AuthKey_*.p8`

4. **No Data**:
   - [ ] Check logs in `.dlt/` directory
   - [ ] Verify apps have data in App Store Connect
   - [ ] Try increasing `APPSTORE_BACKFILL_DAYS`

## 📚 Next Steps

- [ ] Review [APP_STORE_SETUP.md](APP_STORE_SETUP.md) for detailed documentation
- [ ] Set up Metabase dashboards for App Store data
- [ ] Configure alerts for important metrics (reviews, crashes, etc.)
- [ ] Schedule regular backfills if needed

## 🎉 You're Done!

Your App Store Connect ETL pipeline is now set up and running!
