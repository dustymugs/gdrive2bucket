# https://gist.github.com/xflr6/d106aa5b561fbac4ce1a9969eba728bb
"""os.walk() variation with Google Drive API."""

import io
import os.path as osp
import queue
import tempfile
import threading
import time
from types import SimpleNamespace

from apiclient.discovery import build
import click
import google.auth
from google.cloud import storage
from googleapiclient.http import MediaIoBaseDownload

FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'
MIME_TYPE_MAP = {
    'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.google-apps.drawing': 'image/png',
    'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.google-apps.script': 'application/vnd.google-apps.script+json',
}

CONNECT_TIMEOUT = 10 # in seconds
READ_TIMEOUT = 900 # 15 minutes
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB upload chunks

BUCKET = 'bborie-sandbox'

file_queue = queue.Queue()
LOCK = threading.Lock()

class UnsupportedMimeType(Exception):
    pass

def get_credentials(scopes, secrets='./client_secrets.json', storage='./storage.json'):
    from oauth2client import file, client, tools

    store = file.Storage(storage)
    creds = store.get()

    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(secrets, scopes)
        flags = tools.argparser.parse_args([])
        creds = tools.run_flow(flow, store, flags)

    return creds

def clean_value(v):
    return v.replace("'", "\\'")

def iterfiles(service, name=None, is_folder=None, parent=None, drive_id=None, order_by='folder,name,createdTime'):
    q = ['trashed=false']
    if name is not None:
        q.append(f"name='{clean_value(name)}'")
    if is_folder is not None:
        q.append(f"mimeType {'=' if is_folder else '!='} '{FOLDER_MIME_TYPE}'")
    if parent is not None:
        q.append(f"'{clean_value(parent)}' in parents")

    params = {'pageToken': None, 'orderBy': order_by}
    if q:
        params['q'] = ' and '.join(q)

    if drive_id:
        params['driveId'] = drive_id
        params['corpora'] = 'drive'
        params['includeItemsFromAllDrives'] = True
        params['supportsAllDrives'] = True

    while True:
        response = service.files().list(**params).execute()
        for f in response['files']:
            yield f
        try:
            params['pageToken'] = response['nextPageToken']
        except KeyError:
            return

def walk(service, top='root', by_name=False, drive_id=None):
    if by_name:
        top, = iterfiles(service, name=top, is_folder=True, drive_id=drive_id)
    else:
        params = dict(fileId=top)
        if drive_id:
            params['supportsAllDrives'] = True
        top = service.files().get(**params).execute()
        if top['mimeType'] != FOLDER_MIME_TYPE:
            raise ValueError(f'not a folder: {top!r}')

    stack = [((top['name'],), top)]
    while stack:
        path, top = stack.pop()

        dirs, files = is_file = [], []
        for f in iterfiles(service, parent=top['id'], drive_id=drive_id):
            is_file[f['mimeType'] != FOLDER_MIME_TYPE].append(f)

        yield path, top, dirs, files

        if dirs:
            stack.extend((path + (d['name'],), d) for d in reversed(dirs))

def get_drive_service():
    creds = get_credentials(
        scopes=[
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/drive.metadata.readonly',
        ]
    )

    service = build('drive', version='v3', credentials=creds)
    return service

def init_workers(num_workers=8, thread_kwargs=None):
    thread_kwargs = thread_kwargs or {}
    file_workers = [
        threading.Thread(target=copy_file, daemon=True, kwargs=thread_kwargs)
        for x in range(num_workers)
    ]
    list(map(lambda x: x.start(), file_workers))

    return file_workers

def copy_file(project, bucket):
    with LOCK:
        service = get_drive_service()
        client = storage.Client(
            project=project,
            credentials=google.auth.default()[0],
        )
        bucket = client.bucket(bucket)

    while True:
        try:
            f = file_queue.get()
            if f is None:
                break

            _copy_file(f, service, bucket)
        except Exception:
            # TODO
            pass
        finally:
            file_queue.task_done()

def _copy_file(f, service, bucket):
    with tempfile.NamedTemporaryFile() as fh:
        #
        # download
        #

        file = io.FileIO(fh.name, 'w+')
        if '.google-apps.' in f.mimeType:
            mime_type = MIME_TYPE_MAP.get(f.mimeType)
            if not mime_type:
                raise UnsupportedMimeType()

            f.mimeType = mime_type
            req = service.files().export_media(fileId=f.id, mimeType=mime_type)
        else:
            req = service.files().get_media(fileId=f.id)

        downloader = MediaIoBaseDownload(file, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        #
        # upload
        #

        blob = bucket.blob(f.bucketPath, chunk_size=CHUNK_SIZE)
        blob.upload_from_filename(fh.name, timeout=TIMEOUT)
        blob.content_type = f.mimeType
        blob.patch()

        print(f'Copied: {f.id} {f.drivePath} => gs://{bucket.name}/{f.bucketPath}')

@click.command()
@click.option('--project', '-p', required=True, help='Name of GCP Project containing GCP Storage Bucket')
@click.option('--drive', '-d', default="//My Drive/", help='Google Drive Path: //<DRIVE_NAME>/FOLDER/PATH. Use `//My Drive/` for your private Google Drive')
@click.option('--bucket', '-b', required=True, help='Name of existing GCP Storage Bucket where files from Google Drive will be copied')
@click.option('--workers', '-w', default=8, type=int, help='Number of workers to do the copying from Google Drive to Bucket')
def copy_from_gdrive_to_bucket(project, drive, bucket, workers):
    if not drive.startswith('//'):
        drive = '//' + osp.join('My Drive', (drive if drive[0] != '/' else drive[1:]))
        print(f'Assuming "My Drive": {drive}')

    drive_name, drive_prefix = drive[2:].split('/', 1)
    if drive_prefix.endswith('/'):
        drive_prefix = drive_prefix[:-1]
    drive_parts = drive_prefix.split('/')

    drive_id = None
    drive_parent = 'root'

    service = get_drive_service()

    file_workers = init_workers(
        workers,
        thread_kwargs={
            'project': project,
            'bucket': bucket
        }
    )

    if drive_name.strip().lower() != 'my drive':
        # go look up driveId
        response = service.drives().list(q=f"name='{clean_value(drive_name)}'").execute()
        drive_id = next(
            drive_dict['id']
            for drive_dict in response['drives']
        )
        drive_parent = None

    for part in drive_parts:
        for f in iterfiles(service, name=part, parent=drive_parent, is_folder=True, drive_id=drive_id):
            drive_parent = f['id']

    for _path, root, dirs, files in walk(service, top=drive_parent, by_name=False, drive_id=drive_id):
        for f in files:
            f = SimpleNamespace(**f)
            f.drivePath = osp.join(drive, *_path[1:], f.name)
            f.bucketPath = osp.join(drive[2:], *_path[1:], f.name)
            file_queue.put(f)

    while True:
        qsize = file_queue.qsize()
        print(f'Estimate to process: {qsize}')
        if qsize < 5:
            break
        time.sleep(10)

    # gracefully shut down workers
    for x in range(len(file_workers)):
        file_queue.put(None)

    file_queue.join()

    print('All done! Bye...')

if __name__ == '__main__':
    copy_from_gdrive_to_bucket()
