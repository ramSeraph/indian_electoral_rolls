from pathlib import Path

from boto3.s3.transfer import TransferConfig
from utils import (get_boto_client, download_pdf_archive_from_r2,
                   extract_archive, convert_to_pages,
                   create_archive, upload_archive_to_r2)

import threading
import queue

def get_details(key):
    parts = key.split('/')
    scode = parts[0]
    acno  = parts[1]
    lang  = parts[2][:-4]
    return scode, acno, lang


q = queue.Queue(maxsize=3)

def run_conversion():
    while True:
        key = q.get()
        if key == 'DONE':
            q.task_done()
            break
        print(f'processing {key} for conversion')
        scode, acno, lang = get_details(key)
        extract_archive(scode, acno, lang)
        for p in Path(f'data/raw/{key[:-4]}/').glob('*.pdf'):
            print(f'converting file {p}')
            convert_to_pages(p, num_procs=32)

        create_archive(scode, acno, lang)
        upload_archive_to_r2(scode, acno, lang)
        s3.delete_object(Bucket=bucket_name_from, Key=key)
        q.task_done()


s3 = get_boto_client()

bucket_name_from = 'indian-electoral-rolls-pdfs'

threading.Thread(target=run_conversion, daemon=True).start()
threading.Thread(target=run_conversion, daemon=True).start()

response = s3.list_objects(Bucket=bucket_name_from)
for item in response['Contents']:
    key = item['Key']
    if not key.endswith('.tar'):
        continue
    if key == 'raw.tar':
        continue
    scode, acno, lang = get_details(key)
    download_pdf_archive_from_r2(scode, acno, lang)
    q.put(key)
q.put('DONE')
q.join()
