
from boto3.s3.transfer import TransferConfig
from utils import get_boto_client, ProgressDownloadPercentage, MULTIPART_CHUNK_SIZE_MB


s3 = get_boto_client()

bucket_name_from = 'indian-electoral-roll-pdfs'
bucket_name_to = 'indian-electoral-roll-pages'

config = TransferConfig(multipart_threshold=1024*MULTIPART_CHUNK_SIZE_MB, max_concurrency=10,
                        multipart_chunksize=1024*MULTIPART_CHUNK_SIZE_MB, use_threads=True)

response = s3.list_objects(Bucket=bucket_name_from)
for item in response['Contents']:
    key = item['Key']
    if not key.endswith('.tar'):
        continue
    parts = key.split('/')
    scode = parts[0]
    acno  = parts[1]
    lang  = parts[2][:-4]
    download_pdf_archive_from_r2(scode, acno, lang)
    extract_archive(scode, acno, lang)
    for p in Path(f'data/raw/{key}').glob('*.pdf'):
        print(f'converting file {p}')
        convert_to_pages(p)

    create_archive(scode, acno, lang)
    upload_pdf_archive_to_r2(scode, acno, lang)
    s3.delete_object(Bucket=bucket_name_from, Key=key)

    

