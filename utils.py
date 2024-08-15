import io
import sys
import threading
import json
import subprocess
import shutil
from multiprocessing import Pool, cpu_count
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

from pypdf import PdfReader
from PIL import Image


def run_external(cmd):
    #print(f'running cmd - {cmd}')
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    #print(f'STDOUT: {res.stdout}')
    #print(f'STDERR: {res.stderr}')
    if res.returncode != 0:
        raise Exception(f'command {cmd} failed with exit code: {res.returncode}')

def convert_to_webp(arg):
    png_file, webp_file = arg
    cmd = f'cwebp -q 100 -z 9 -lossless {png_file} -o {webp_file}'
    try:
        run_external(cmd)
    except Exception as ex:
        print(ex)
        raise ex
    png_file.unlink()

def get_alt_dir(file, alt):
    parents = list(file.parents)
    parents.reverse()
    pieces = [ p.name for p in parents ]
    r_indices = [i for i, x in enumerate(pieces) if x == "raw"]
    rindex = r_indices[-1]
    base_dir = parents[rindex-1]
    alt_dir = base_dir.joinpath(*([alt] + pieces[rindex+1:] + [file.name[:-4]]))
    return alt_dir

def extract_images_from_pdf(file, pages_dir, num_procs):
    pages_dir.mkdir(exist_ok=True, parents=True)
    reader = PdfReader(file)
    pno = 1
    page_png_files = []
    page_files = []
    for page in reader.pages:
        images = page.images
        num_images = len(images)
        if num_images != 1:
            raise Exception(f'Found {num_images} on {pno}')

        page_png_file = pages_dir / f'{pno}.png'
        page_file = pages_dir / f'{pno}.webp'
        print(f'\t\t\t\twriting page - {pno}')
        page_png_file.write_bytes(images[0].data)
        page_png_files.append(page_png_file)
        page_files.append(page_file)
        pno += 1

    args = zip(page_png_files, page_files)
    if num_procs == -1:
        for arg in args:
            convert_to_webp(arg)
    else:
        with Pool(num_procs) as pool:
            results = pool.map(convert_to_webp, args)


def convert_to_pages(pdf_file, num_procs=cpu_count()*2):
    sz = pdf_file.stat().st_size
    if sz <= 4:
        return
    pages_dir = get_alt_dir(pdf_file, 'pages')
    extract_images_from_pdf(pdf_file, pages_dir, num_procs)
    pdf_file.write_text('DONE')


class ProgressDownloadPercentage(object):
    def __init__(self, size):
        self._size = float(size)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r %s / %s  (%.2f%%)" % (
                    self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()



class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(Path(filename).stat().st_size)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


boto_client = None
def get_boto_client():
    global boto_client
    if boto_client is None:
        config = json.loads(Path('infra/r2_credentials.json').read_text())
        boto_client = boto3.client('s3',
                                   endpoint_url = f'https://{config["accountid"]}.r2.cloudflarestorage.com',
                                   aws_access_key_id = config['access_key_id'],
                                   aws_secret_access_key = config['access_key_secret'])
    return boto_client


def create_pdf_archive(scode, acno, lang):
    ac_pdfs_dir = Path('data/raw/') / f'{scode}' / f'{acno}'
    l_pdfs_dir  = ac_pdfs_dir / f'{lang}' 
    if not l_pdfs_dir.exists():
        return

    archive_file = ac_pdfs_dir / f'{lang}.tar'

    if not archive_file.exists():
        print(f'creating {archive_file}')
        cmd = f'tar -cvf {archive_file} {l_pdfs_dir}'
        print(f'running - {cmd}')
        run_external(cmd)
        print(f'deleting {l_pdfs_dir}')
        shutil.rmtree(l_pdfs_dir)



def create_archive(scode, acno, lang):
    ac_pages_dir = Path('data/pages/') / f'{scode}' / f'{acno}'
    l_pages_dir  = ac_pages_dir / f'{lang}' 
    archive_file = ac_pages_dir / f'{lang}.tar'

    if not l_pages_dir.exists():
        return


    if not archive_file.exists():
        print(f'creating {archive_file}')
        cmd = f'tar -cvf {archive_file} {l_pages_dir}'
        print(f'running - {cmd}')
        run_external(cmd)
        print(f'deleting {l_pages_dir}')
        shutil.rmtree(l_pages_dir)

MULTIPART_CHUNK_SIZE_MB = 100

def upload_pdf_archive_to_r2(scode, acno, lang):
    ac_pdfs_dir = Path('data/raw/') / f'{scode}' / f'{acno}'
    archive_file = ac_pdfs_dir / f'{lang}.tar'

    if not archive_file.exists():
        return
       
    s3 = get_boto_client()
    config = TransferConfig(multipart_threshold=1024*MULTIPART_CHUNK_SIZE_MB, max_concurrency=10,
                            multipart_chunksize=1024*MULTIPART_CHUNK_SIZE_MB, use_threads=True)

    print(f'uploading {archive_file}')
    s3.upload_file(archive_file, 'indian-electoral-rolls-pdfs', f'{scode}/{acno}/{lang}.tar',
                   Config=config, Callback=ProgressPercentage(archive_file))

    print(f'deleting {archive_file}')
    archive_file.unlink()

def upload_archive_to_r2(scode, acno, lang):

    ac_pages_dir = Path('data/pages/') / f'{scode}' / f'{acno}'
    archive_file = ac_pages_dir / f'{lang}.tar'

    if not archive_file.exists():
        return
       
    s3 = get_boto_client()
    config = TransferConfig(multipart_threshold=1024*MULTIPART_CHUNK_SIZE_MB, max_concurrency=10,
                            multipart_chunksize=1024*MULTIPART_CHUNK_SIZE_MB, use_threads=True)

    print(f'uploading {archive_file}')
    bucket_name = 'indian-electoral-rolls'
    key = f'{scode}/{acno}/{lang}.tar'
    s3.upload_file(archive_file, bucket_name, key,
                   Config=config, Callback=ProgressPercentage(archive_file))

    print(f'deleting {archive_file}')
    archive_file.unlink()

def download_pdf_archive_from_r2(scode, acno, lang):
    ac_pdfs_dir = Path('data/raw/') / f'{scode}' / f'{acno}'
    archive_file = ac_pdfs_dir / f'{lang}.tar'

    if archive_file.exists():
        return

    ac_pdfs_dir.mkdir(parents=True, exist_ok=True)
    bucket_name = 'indian-electoral-rolls-pdfs'
    key = f'{scode}/{acno}/{lang}.tar'

    s3 = get_boto_client()
    config = TransferConfig(multipart_threshold=1024*MULTIPART_CHUNK_SIZE_MB, max_concurrency=10,
                            multipart_chunksize=1024*MULTIPART_CHUNK_SIZE_MB, use_threads=True)
    print(f'downloading {archive_file}')
    response = s3.head_object(Bucket=bucket_name, Key=key)
    archive_size = response['ContentLength']

    s3.download_file(bucket_name, key, archive_file,
                     Config=config, Callback=ProgressDownloadPercentage(archive_size))



def extract_archive(scode, acno, lang):
    ac_pdfs_dir = Path('data/raw/') / f'{scode}' / f'{acno}'
    l_pdfs_dir  = ac_pdfs_dir / f'{lang}'
    if l_pdfs_dir.exists():
        return

    archive_file = ac_pdfs_dir / f'{lang}.tar'

    if archive_file.exists():
        print(f'extracting {archive_file}')
        cmd = f'tar -xf {archive_file}'
        print(f'running - {cmd}')
        run_external(cmd)
        print(f'deleting {archive_file}')
        archive_file.unlink()

def get_bucket_keys(bucket_name):
    keys = set()
    s3 = get_boto_client()
    response = s3.list_objects(Bucket=bucket_name)
    for item in response['Contents']:
        key = item['Key']
        keys.add(key)
    return keys


