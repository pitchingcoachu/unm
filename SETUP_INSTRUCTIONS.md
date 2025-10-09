# VMI Baseball Data Automation Setup Instructions

This guide will help you set up automatic data synchronization from TrackMan FTP to your Shiny app deployed on shinyapps.io.

## Overview

The automation system will:
- **Daily at 10 AM UTC (6 AM EST)**: Automatically sync data from TrackMan FTP
- **Filter efficiently**: Only download and keep VMI data (VIR_KEY/VMI_KEY)
- **Fast processing**: Downloads in batches, filters during download
- **Auto-deploy**: Only redeploys your app when new data is found
- **Version control**: Keeps track of all data changes in GitHub

## Files Created

The following files have been created in your VMI folder:

1. **`automated_data_sync.R`** - Script that syncs data from FTP
2. **`deploy_script.R`** - Script that deploys app to shinyapps.io
3. **`.github/workflows/daily-sync.yml`** - GitHub Actions workflow
4. **`data/`** - Directory where synced data will be stored
5. **Updated `app.R`** - Now loads data from the data directory

## Setup Steps

### Step 1: Create GitHub Repository

1. Go to [GitHub.com](https://github.com) and create a new repository
2. Name it something like "vmi-baseball-app"
3. Make it public (for free GitHub Actions) or private if you have a paid plan
4. **Do NOT** initialize with README, .gitignore, or license (we'll upload your existing files)

### Step 2: Upload Your Files to GitHub

1. In your new GitHub repository, click "uploading an existing file"
2. Upload ALL files from your VMI folder:
   - `app.R`
   - `automated_data_sync.R`
   - `deploy_script.R`
   - The entire `.github` folder
   - The entire `data` folder
3. Commit the files with message "Initial VMI app setup"

### Step 3: Get Your shinyapps.io Credentials

Run this in R to get your credentials:

```r
# Install rsconnect if you haven't
install.packages("rsconnect")
library(rsconnect)

# This will show your account info
showUsers()
```

You'll see output like:
```
      name   server
1 yourname rsconnect.shinyapps.io

Account: yourname
Server: rsconnect.shinyapps.io
Token: ABC123...
Secret: XYZ789...
```

### Step 4: Add GitHub Secrets

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** and add these three secrets:

   - **Name**: `SHINYAPPS_ACCOUNT`  
     **Value**: Your account name from showUsers()
   
   - **Name**: `SHINYAPPS_TOKEN`  
     **Value**: Your token from showUsers()
   
   - **Name**: `SHINYAPPS_SECRET`  
     **Value**: Your secret from showUsers()

### Step 5: Test the Workflow

1. Go to the **Actions** tab in your GitHub repository
2. Click on "Daily VMI Data Sync and Deploy"
3. Click **"Run workflow"** to test it manually
4. Watch the workflow run - it should:
   - Sync data from TrackMan FTP
   - Deploy your app to shinyapps.io
   - Commit any new data to your repository

### Step 6: Update App Name (Optional)

If you want to change the app name on shinyapps.io:

1. Edit `deploy_script.R`
2. Change the line: `appName = "vmi-baseball-analytics"`
3. Choose your preferred app name
4. Commit the change to GitHub

## How It Works

### Daily Automation
- **10 AM UTC (6 AM EST)**: GitHub Actions runs automatically
- Downloads only 2025 data from `/practice/2025/` and `/v3/2025/`
- Filters each CSV file for VMI data only (VIR_KEY/VMI_KEY)
- Saves filtered data to your GitHub repository
- Deploys updated app to shinyapps.io
- Only redeploys if new VMI data is found

### Data Processing
- **Practice folder**: Downloads all CSV files (VMI-specific)
- **V3 folder**: Downloads all CSV files but filters for VMI only
- **Batch processing**: Handles large numbers of files efficiently
- **Memory management**: Processes files in small batches

### Your App
- **Data loading**: Now automatically loads from the `data/` directory
- **Graceful fallback**: Shows empty data frame if no files found
- **Multiple files**: Combines all CSV files into one dataset

## Monitoring

### Check if it's working:
1. **GitHub Actions**: Go to Actions tab to see workflow runs
2. **shinyapps.io**: Check your app dashboard for deployments
3. **Data folder**: New CSV files should appear in your GitHub repo

### Manual trigger:
- Go to Actions → Daily VMI Data Sync and Deploy → Run workflow

### Troubleshooting:
- Check GitHub Actions logs for detailed error messages
- Verify your shinyapps.io secrets are correct
- Make sure FTP credentials are working

## Cost

- **GitHub Actions**: Free for public repositories
- **shinyapps.io**: Uses your existing plan
- **TrackMan FTP**: Uses your existing access

## Security

- FTP credentials are in the script files (consider moving to GitHub secrets for production)
- shinyapps.io credentials are securely stored in GitHub secrets
- All code and data are in your private GitHub repository

## Support

If you encounter issues:
1. Check the GitHub Actions logs first
2. Verify your shinyapps.io credentials
3. Test the sync script locally: `Rscript automated_data_sync.R`
4. Test deployment locally: `Rscript deploy_script.R`

## Manual Usage

You can also run the scripts manually:

```bash
# Sync data only
Rscript automated_data_sync.R

# Deploy app only  
Rscript deploy_script.R
```

The automation is now complete! Your VMI baseball app will automatically stay updated with the latest TrackMan data.
