# App Store Connect Setup Guide

This guide walks you through setting up the App Store Connect ETL pipeline to extract data from your iOS/macOS apps.

## Prerequisites

- Apple Developer Program membership ($99/year)
- Access to App Store Connect
- Admin or App Manager role in App Store Connect
- Python 3.9+ installed locally

## Step 1: Generate App Store Connect API Key

### 1.1 Create API Key

1. Log in to [App Store Connect](https://appstoreconnect.apple.com/)
2. Click on **Users and Access** in the top navigation
3. Click on the **Keys** tab (under Integrations)
4. Click the **+** button to generate a new API key
5. Enter a name for your key (e.g., "ETL Pipeline Key")
6. Select **Access** level:
   - **Admin**: Full access (recommended for complete data access)
   - **App Manager**: Access to app data, sales, and analytics
   - **Developer**: Limited access
7. Click **Generate**

### 1.2 Download and Save the Private Key

⚠️ **IMPORTANT**: You can only download the private key **ONCE**. Store it securely!

1. After generating, click **Download API Key**
2. Save the `.p8` file (e.g., `AuthKey_XXXXXXXXXX.p8`) to a secure location
3. **Note the following values** (you'll need them later):
   - **Issuer ID**: Found at the top of the Keys page (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
   - **Key ID**: 10-character string shown in the key list (format: `XXXXXXXXXX`)

### 1.3 Store the Private Key Securely

```bash
# Create a secure directory for keys
mkdir -p ~/.app-store-keys
chmod 700 ~/.app-store-keys

# Move the downloaded key
mv ~/Downloads/AuthKey_XXXXXXXXXX.p8 ~/.app-store-keys/
chmod 600 ~/.app-store-keys/AuthKey_XXXXXXXXXX.p8
```

## Step 2: Find Your App IDs

### 2.1 Get App IDs from App Store Connect

1. Go to [App Store Connect](https://appstoreconnect.apple.com/)
2. Click on **My Apps**
3. Select an app
4. In the **App Information** section (left sidebar), find the **Apple ID**
   - This is a numeric ID (e.g., `1234567890`)

Repeat for all apps you want to track.

## Step 3: Configure the Pipeline

### 3.1 Create Configuration File

Copy the example configuration:

```bash
cp app_store.json.example app_store.json
```

### 3.2 Edit Configuration

Edit `app_store.json` with your credentials:

```json
{
  "d1": {
    "issuer_id": "12345678-1234-1234-1234-123456789012",
    "key_id": "ABCDE12345",
    "private_key_path": "/Users/yourname/.app-store-keys/AuthKey_ABCDE12345.p8",
    "apps": [
      {
        "app_id": "1234567890",
        "app_name": "My Awesome App"
      },
      {
        "app_id": "0987654321",
        "app_name": "Another Great App"
      }
    ]
  }
}
```

**Configuration Fields:**
- `issuer_id`: Your Issuer ID from App Store Connect
- `key_id`: Your Key ID (10 characters)
- `private_key_path`: Full path to your `.p8` private key file
- `apps`: Array of apps to track
  - `app_id`: The Apple ID of the app
  - `app_name`: Human-readable name (for logging and metadata)

### 3.3 Add to .gitignore

⚠️ **CRITICAL**: Never commit credentials to git!

Ensure `app_store.json` is in `.gitignore`:

```bash
echo "app_store.json" >> .gitignore
```

## Step 4: Install Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

Additional dependencies for App Store Connect:

```bash
pip install PyJWT cryptography requests
```

## Step 5: Test the Pipeline Locally

### 5.1 Run a Test

```bash
# Run for the d1 group
python main.py app_store d1
```

### 5.2 Verify Data

Check ClickHouse for the loaded data:

```sql
-- Check loaded tables
SHOW TABLES FROM app_store;

-- View sample data
SELECT * FROM app_store.apps LIMIT 10;
SELECT * FROM app_store.customer_reviews LIMIT 10;
SELECT * FROM app_store.app_store_versions LIMIT 10;
```

## Step 6: Configure GitHub Actions (Optional)

### 6.1 Prepare Secrets

For automated runs, you need to set up GitHub Secrets.

**Create `APP_STORE_CONFIG` secret:**

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `APP_STORE_CONFIG`
5. Value: Entire contents of your `app_store.json` file

**Note**: For the private key in GitHub Actions, you have two options:

**Option A: Inline Private Key (Recommended)**

Modify your config to include the private key directly:

```json
{
  "d1": {
    "issuer_id": "12345678-1234-1234-1234-123456789012",
    "key_id": "ABCDE12345",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQg...\n-----END PRIVATE KEY-----",
    "apps": [...]
  }
}
```

**Option B: Use GitHub Secrets for Key Path**

Store the key content in a separate secret and write it to a file in the workflow.

### 6.2 Verify Workflow

The workflows are already created:
- `.github/workflows/app-store-daily.yml` - Daily automated runs
- `.github/workflows/app-store-backfill.yml` - Manual backfill runs

## Step 7: Data Available

The pipeline extracts the following data:

| Table | Description | Update Frequency |
|-------|-------------|------------------|
| `apps` | App metadata and basic info | Daily |
| `app_info` | Detailed app information with localizations | Daily |
| `customer_reviews` | User reviews and ratings | Daily (incremental) |
| `app_store_versions` | Version history and release info | Daily |
| `builds` | Build information and TestFlight data | Daily (incremental) |
| `in_app_purchases` | IAP products and pricing | Daily |
| `beta_testers` | TestFlight beta tester information | Daily |

## Troubleshooting

### Authentication Errors (401)

**Error**: `401 Unauthorized` or `NOT_AUTHORIZED`

**Solutions**:
1. Verify your `issuer_id` and `key_id` are correct
2. Check that the private key file path is correct
3. Ensure the API key has sufficient permissions (Admin or App Manager)
4. Verify the private key format includes header/footer:
   ```
   -----BEGIN PRIVATE KEY-----
   ...
   -----END PRIVATE KEY-----
   ```

### Rate Limiting (429)

**Error**: `429 Too Many Requests`

**Solutions**:
1. The pipeline includes automatic retry with backoff
2. Reduce the number of apps processed simultaneously
3. Increase delays between requests if needed

### App Not Found (404)

**Error**: `404 Not Found` for app endpoint

**Solutions**:
1. Verify the `app_id` is correct (numeric Apple ID)
2. Ensure the API key has access to the app
3. Check that the app exists in your App Store Connect account

### Private Key Format Issues

**Error**: `Could not deserialize key data` or `Invalid key format`

**Solutions**:
1. Ensure the private key includes the full PEM format:
   ```
   -----BEGIN PRIVATE KEY-----
   MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQg...
   -----END PRIVATE KEY-----
   ```
2. Check for extra whitespace or newlines
3. Verify the file is readable: `cat ~/.app-store-keys/AuthKey_*.p8`

### No Data Returned

**Possible Causes**:
1. App has no data for the requested resource (e.g., no reviews, no beta testers)
2. Date filters are too restrictive
3. App is not yet published

**Solutions**:
1. Check logs for specific errors
2. Try increasing `days_back` parameter
3. Verify data exists in App Store Connect web interface

## Advanced Configuration

### Backfill Historical Data

To load historical data:

```bash
# Backfill 90 days of data
APPSTORE_BACKFILL_DAYS=90 python main.py app_store d1
```

### Custom Pipeline Names

For parallel runs or testing:

```bash
# Use a custom pipeline name suffix
PIPELINE_NAME_SUFFIX=_test python main.py app_store d1
```

### Multiple Groups

You can configure multiple groups for different teams or app collections:

```json
{
  "d1": {
    "issuer_id": "...",
    "key_id": "...",
    "apps": [...]
  },
  "m4": {
    "issuer_id": "...",
    "key_id": "...",
    "apps": [...]
  }
}
```

Run each group separately:

```bash
python main.py app_store d1
python main.py app_store m4
```

## Security Best Practices

1. **Never commit** `app_store.json` or `.p8` files to git
2. **Restrict file permissions** on private keys: `chmod 600`
3. **Rotate API keys** periodically (every 6-12 months)
4. **Use separate keys** for production and development
5. **Monitor API usage** in App Store Connect
6. **Revoke unused keys** immediately

## API Limits and Quotas

- **Rate Limits**: Apple doesn't publish specific limits, but be reasonable
- **Token Expiration**: JWT tokens expire after 20 minutes (handled automatically)
- **Data Retention**: 
  - Sales data: 1 year
  - Analytics data: 2 years
  - Metadata: Indefinite

## Additional Resources

- [App Store Connect API Documentation](https://developer.apple.com/documentation/appstoreconnectapi)
- [Generating API Keys](https://developer.apple.com/documentation/appstoreconnectapi/creating_api_keys_for_app_store_connect_api)
- [JWT Token Generation](https://developer.apple.com/documentation/appstoreconnectapi/generating_tokens_for_api_requests)
- [App Store Connect Help](https://help.apple.com/app-store-connect/)

## Support

For issues specific to this pipeline:
1. Check the logs in `.dlt/` directory
2. Review the troubleshooting section above
3. Verify your configuration matches the example

For App Store Connect API issues:
- [Apple Developer Forums](https://developer.apple.com/forums/)
- [App Store Connect Support](https://developer.apple.com/contact/)
