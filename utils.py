import io
import subprocess
from multiprocessing import Pool, cpu_count
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
    run_external(cmd)
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

def extract_images_from_pdf(file, pages_dir):
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
    with Pool(cpu_count()) as pool:
        results = pool.map(convert_to_webp, args)


def convert_to_pages(pdf_file):
    sz = pdf_file.stat().st_size
    if sz <= 4:
        return
    pages_dir = get_alt_dir(pdf_file, 'pages')
    extract_images_from_pdf(pdf_file, pages_dir)
    pdf_file.write_text('DONE')
 
