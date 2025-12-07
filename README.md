# GCS to Google Drive Transfer

A high-performance Python script to transfer an entire Google Cloud Storage (GCS) bucket to a Google Drive folder.

## Features

- **Parallel Transfer**: Uses multi-threading to transfer files quickly.
- **Incremental**: Skips files that already exist in the target Drive folder.
- **Smart Structure**: Recreates the GCS directory structure in Google Drive.
- **Robust Auth**: Handles OAuth scopes, Project IDs, and Quota projects automatically.

## Prerequisites

- Python 3.7+
- A Google Cloud Project

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/kousiktn/gcs2gdrive.git
   cd gcs2gdrive
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Authentication Setup

This script requires access to both Google Cloud Storage and Google Drive. 

### 1. Enable APIs
Enable the Google Drive API for your Google Cloud project:
```bash
gcloud services enable drive.googleapis.com --project <YOUR_PROJECT_ID>
```

### 2. Authenticate with Scopes
Authenticate `gcloud` with the necessary permissions (Drive + Cloud Platform):
```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform
```

## Usage

### Basic Usage
Run the script providing your bucket name, target Drive folder, and Project ID (for billing/quotas).

```bash
python gcs_to_drive.py \
  --bucket <GCS_BUCKET_NAME> \
  --drive-folder <TARGET_DRIVE_FOLDER_NAME> \
  --project <YOUR_PROJECT_ID>
```

### Advanced Usage
Maximize performance with more workers (default is 10):

```bash
python gcs_to_drive.py \
  --bucket my-large-bucket \
  --drive-folder "Backup 2024" \
  --project my-gcp-project \
  --workers 20
```

### Options
| Flag | Description | Required | Default |
|------|-------------|:--------:|:-------:|
| `--bucket` | Source GCS Bucket Name | Yes | - |
| `--drive-folder` | Target Folder Name in Google Drive | Yes | - |
| `--project` | Google Cloud Project ID (for API quotas) | Yes (for User Auth) | - |
| `--workers` | Number of parallel transfer threads | No | 10 |
| `--gcs-sa` | Path to GCS Service Account JSON | No | - |
| `--drive-sa` | Path to Drive Service Account JSON | No | - |

## Troubleshooting

### "Invalid Value" or 400 Errors
If you see errors related to file names containing special characters (like `'`), ensure you have the latest version of the script which handles proper character escaping.

### "Insufficient Authentication Scopes"
If you see this error, you likely authenticated without the Drive scope. Re-run:
```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform
```
