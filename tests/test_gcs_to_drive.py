import unittest
from unittest.mock import MagicMock, patch, call, ANY
import sys
import os
import concurrent.futures

# Add parent directory to path to import script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from gcs_to_drive import transfer_bucket, find_or_create_folder, transfer_blob, get_drive_service

class TestGCSToDrive(unittest.TestCase):

    def setUp(self):
        self.mock_gcs_client = MagicMock()
        self.mock_drive_service = MagicMock()
        self.mock_drive_creds = MagicMock()

    @patch('gcs_to_drive.get_gcs_client')
    @patch('gcs_to_drive.get_drive_creds')
    @patch('gcs_to_drive.get_drive_service')
    def test_transfer_bucket_empty(self, mock_get_drive_svc, mock_get_drive_creds, mock_get_gcs):
        mock_get_gcs.return_value = self.mock_gcs_client
        mock_get_drive_svc.return_value = self.mock_drive_service
        mock_get_drive_creds.return_value = self.mock_drive_creds
        
        # Mock empty bucket
        bucket_mock = MagicMock()
        bucket_mock.list_blobs.return_value = []
        self.mock_gcs_client.bucket.return_value = bucket_mock

        transfer_bucket('test-bucket', 'Target')
        
        self.mock_drive_service.files().list.assert_not_called()

    @patch('gcs_to_drive.get_drive_service')
    def test_transfer_blob_single(self, mock_get_drive_svc):
        # Test the worker function in isolation
        mock_service = MagicMock()
        mock_get_drive_svc.return_value = mock_service
        
        blob_mock = MagicMock()
        blob_mock.name = 'folder/file.txt'
        blob_mock.content_type = 'text/plain'
        
        drive_creds = MagicMock()
        root_folder_id = 'root_123'
        folder_cache = {}
        
        # Responses for find_or_create_folder logic inside ensure_folder_structure
        # 1. Search 'folder' in 'root_123' -> Not Found
        # 2. Create 'folder' -> 'folder_123'
        # 3. Check 'file.txt' in 'folder_123' -> Not Found
        # 4. Create 'file.txt'
        
        mock_service.files().list.return_value.execute.side_effect = [
            {'files': []}, # Folder search
            {'files': []}  # File search
        ]
        
        mock_service.files().create.return_value.execute.side_effect = [
            {'id': 'folder_123'},
            {'id': 'file_123'}
        ]

        transfer_blob(blob_mock, drive_creds, root_folder_id, folder_cache)
        
        # Verify folder creation
        mock_service.files().create.assert_any_call(
            body={'name': 'folder', 'mimeType': 'application/vnd.google-apps.folder', 'parents': ['root_123']},
            fields='id'
        )
        # Verify file creation
        # Using ANY because media_body object is hard to match equality on
        mock_service.files().create.assert_called_with(
            body={'name': 'file.txt', 'parents': ['folder_123']},
            media_body=ANY,
            fields='id'
        )

    @patch('gcs_to_drive.get_gcs_client')
    @patch('gcs_to_drive.get_drive_creds')
    @patch('gcs_to_drive.get_drive_service')
    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_transfer_bucket_integration(self, mock_executor, mock_get_drive_svc, mock_get_drive_creds, mock_get_gcs):
        # This tests that we submit tasks to executor
        mock_get_gcs.return_value = self.mock_gcs_client
        mock_get_drive_svc.return_value = self.mock_drive_service
        
        bucket_mock = MagicMock()
        blob1 = MagicMock(name='b1'); blob1.name='a.txt'
        blob2 = MagicMock(name='b2'); blob2.name='b.txt'
        bucket_mock.list_blobs.return_value = [blob1, blob2]
        self.mock_gcs_client.bucket.return_value = bucket_mock
        
        # Mock finding root folder
        self.mock_drive_service.files().list.return_value.execute.return_value = {'files': [{'id': 'root_1', 'name': 'Rot'}]}
        
        # Mock Executor Context Manager
        mock_executor_instance = mock_executor.return_value
        mock_executor_instance.__enter__.return_value = mock_executor_instance
        
        transfer_bucket('test-bucket', 'Target-Folder')
        
        # Verify submit was called twice
        self.assertEqual(mock_executor_instance.submit.call_count, 2)

class TestAuth(unittest.TestCase):
    @patch('gcs_to_drive.google.auth.default')
    def test_get_drive_creds_default(self, mock_auth_default):
        from gcs_to_drive import get_drive_creds
        mock_auth_default.return_value = (MagicMock(), 'project_id')
        
        get_drive_creds(project='my-proj')
        
        mock_auth_default.assert_called_once_with(
            scopes=['https://www.googleapis.com/auth/drive'],
            quota_project_id='my-proj'
        )

if __name__ == '__main__':
    unittest.main()
