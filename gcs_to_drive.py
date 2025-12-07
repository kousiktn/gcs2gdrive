import os
import argparse
import io
import mimetypes
import concurrent.futures
import threading
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
from tqdm import tqdm

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from googleapiclient.errors import HttpError

# Lock to ensure folder creation is thread-safe
FOLDER_LOCK = threading.Lock()

def get_gcs_client(service_account_path=None, project=None):
    """Initializes and returns a GCS client."""
    if service_account_path:
        return storage.Client.from_service_account_json(service_account_path)
    
    # Use ADC but ensure we catch errors downstream if not logged in
    return storage.Client(project=project)

def get_drive_creds(service_account_path=None, project=None):
    """Returns credentials for Drive API."""
    if service_account_path:
        return service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
    
    # Use Application Default Credentials
    creds, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/drive'],
        quota_project_id=project
    )
    return creds

def get_drive_service(creds):
    """Initializes and returns a Google Drive service from creds."""
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def find_or_create_folder(service, folder_name, parent_id=None):
    """Finds a folder by name in a parent folder, or creates it if it doesn't exist."""
    # Escape single quotes in name for the query
    safe_name = folder_name.replace("'", "\\'")
    query = f"mimeType='application/vnd.google-apps.folder' and name='{safe_name}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if files:
        return files[0]['id']
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        file = service.files().create(body=file_metadata, fields='id').execute()
        return file.get('id')

def ensure_folder_structure(drive_service, root_folder_id, path_parts, folder_cache):
    """
    Thread-safe method to ensure the directory structure exists.
    Returns the parent_id for the file.
    """
    current_parent_id = root_folder_id
    current_path_str = ""

    for part in path_parts:
        current_path_str = f"{current_path_str}/{part}" if current_path_str else part
        
        # Check cache first (fast path)
        with FOLDER_LOCK:
            if current_path_str in folder_cache:
                current_parent_id = folder_cache[current_path_str]
                continue

        # Slow path: Check/Create with Lock to prevent duplicates from race conditions
        with FOLDER_LOCK:
            # Double check inside lock
            if current_path_str in folder_cache:
                current_parent_id = folder_cache[current_path_str]
            else:
                # We reuse the passed service inside lock which halts other threads, 
                # but folder creation is rare compared to file upload.
                current_parent_id = find_or_create_folder(drive_service, part, current_parent_id)
                folder_cache[current_path_str] = current_parent_id
    
    return current_parent_id

def transfer_blob(blob, drive_creds, root_folder_id, folder_cache):
    """
    Transfers a single blob to Drive.
    Builds its own Drive service to ensure thread safety.
    """
    try:
        # Build a fresh service instance per thread/worker
        drive_service = get_drive_service(drive_creds)

        path_parts = blob.name.split('/')
        file_name = path_parts[-1]
        
        # If directory placeholder, skip
        if blob.name.endswith('/'):
            return

        # Ensure folders exist
        parent_id = ensure_folder_structure(drive_service, root_folder_id, path_parts[:-1], folder_cache)

        # Check if file exists
        safe_name = file_name.replace("'", "\\'")
        query = f"name='{safe_name}' and '{parent_id}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id)").execute()
        if results.get('files'):
            return # Skip existing

        # Download from GCS
        file_obj = io.BytesIO()
        blob.download_to_file(file_obj)
        file_obj.seek(0)

        # Upload to Drive
        media = MediaIoBaseUpload(file_obj, mimetype=blob.content_type or 'application/octet-stream', resumable=True)
        file_metadata = {'name': file_name, 'parents': [parent_id]}
        
        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
    except Exception as e:
        print(f"Error processing {blob.name}: {e}")
        raise e

def transfer_bucket(bucket_name, drive_root_folder_name, gcs_sa=None, drive_sa=None, project=None, max_workers=10):
    """Transfers an entire GCS bucket to a Google Drive folder using parallel workers."""
    print(f"Initializing transfer from GCS bucket '{bucket_name}' to Drive folder '{drive_root_folder_name}' with {max_workers} workers...")
    
    # 1. Setup Clients
    storage_client = get_gcs_client(gcs_sa, project)
    drive_creds = get_drive_creds(drive_sa, project)
    
    # We use one main service for setup, workers create their own
    main_drive_service = get_drive_service(drive_creds)
    
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    
    if not blobs:
        print("Bucket is empty.")
        return

    # 2. Get/Create Root Folder
    root_folder_id = find_or_create_folder(main_drive_service, drive_root_folder_name)
    print(f"Goal Drive Folder ID: {root_folder_id}")

    # Shared cache structure
    folder_cache = {}

    # 3. Process Blobs in Parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(transfer_blob, blob, drive_creds, root_folder_id, folder_cache): blob for blob in blobs}
        
        # Monitor progress
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(blobs), desc="Transferring files"):
            blob = futures[future]
            try:
                future.result()
            except Exception as e:
                # Error is already printed in worker, but we can log here too if needed
                pass
        
    print("\nTransfer complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer GCS bucket to Google Drive.")
    parser.add_argument("--bucket", required=True, help="GCS Bucket Name")
    parser.add_argument("--drive-folder", required=True, help="Target Folder Name in Google Drive")
    parser.add_argument("--gcs-sa", help="Path to GCS Service Account JSON (optional)")
    parser.add_argument("--drive-sa", help="Path to Drive Service Account JSON (optional)")
    parser.add_argument("--project", help="Google Cloud Project ID (required for ADC/User Credentials)")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers (default: 10)")
    
    args = parser.parse_args()
    
    try:
        transfer_bucket(args.bucket, args.drive_folder, args.gcs_sa, args.drive_sa, args.project, args.workers)
    except DefaultCredentialsError:
        print("\n\033[91mError: Google Cloud credentials not found.\033[0m")
        print("Please authenticate by running:")
        print("  \033[1mgcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform\033[0m")
        print("Or provide a service account file using --gcs-sa and --drive-sa.\n")
    except HttpError as e:
        if e.resp.status == 403 and "insufficient authentication scopes" in str(e).lower():
            print(f"\n\033[91mError: Insufficient Authentication Scopes.\033[0m")
            print("Your current credentials do not have access to Google Drive.")
            print("Please re-authenticate with the required scopes:")
            print("  \033[1mgcloud auth application-default login --scopes=https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform\033[0m\n")
        else:
            raise e
    except Exception as e:
        error_str = str(e).lower()
        if "could not be determined" in error_str or "project" in error_str: 
             print(f"\n\033[91mError: {e}\033[0m")
             print("It seems the Google Cloud Project ID could not be determined.")
             print("Please try running with \033[1m--project <YOUR_PROJECT_ID>\033[0m")
             print("Or set the quota project via: \033[1mgcloud auth application-default set-quota-project <PROJECT_ID>\033[0m\n")
        else:
            raise e
