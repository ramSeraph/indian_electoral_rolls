import json
from pathlib import Path
from utils import (create_archive, upload_archive_to_r2,
                   create_pdf_archive, upload_pdf_archive_to_r2)

raw_dir = Path('data/raw')

def archive_pages():
    for ldir in raw_dir.glob('*/*/*/'):
        if not ldir.is_dir():
            continue
        list_file = ldir.parent.parent / 'constituency_list.json'
        
        parts_file = ldir.parent / 'parts.json'
        parts = json.loads(parts_file.read_text())
        part_files = [ ldir / f'{p["partNumber"]}.pdf' for p in parts ]
        all_parts_done = all([ p.exists() and p.stat().st_size <= 4 for p in part_files ])
        if not all_parts_done:
            continue
        acno = ldir.parent.name
    
        curr_cinfo = None
        cinfos = json.loads(list_file.read_text())
        for cinfo in cinfos:
            if cinfo['asmblyNo'] == int(acno):
                curr_cinfo = cinfo
                break
    
        print(f'archiving {ldir}')
        scode = curr_cinfo['stateCd']
        acno  = curr_cinfo['asmblyNo']
        create_archive(scode, acno, ldir.name)
        upload_archive_to_r2(scode, acno, ldir.name)


def archive_pdfs():
    for ldir in raw_dir.glob('*/*/*/'):
        if not ldir.is_dir():
            continue
        list_file = ldir.parent.parent / 'constituency_list.json'
        
        parts_file = ldir.parent / 'parts.json'
        parts = json.loads(parts_file.read_text())
        part_files = [ ldir / f'{p["partNumber"]}.pdf' for p in parts ]
        all_parts_done = all([ p.exists() and (p.stat().st_size > 4 or p.stat().st_size == 0) for p in part_files ])
        if not all_parts_done:
            continue

        acno = ldir.parent.name
    
        curr_cinfo = None
        cinfos = json.loads(list_file.read_text())
        for cinfo in cinfos:
            if cinfo['asmblyNo'] == int(acno):
                curr_cinfo = cinfo
                break
    
        print(f'archiving {ldir}')
        scode = curr_cinfo['stateCd']
        acno  = curr_cinfo['asmblyNo']
        create_pdf_archive(scode, acno, ldir.name)
        upload_pdf_archive_to_r2(scode, acno, ldir.name)


if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'pages':
        archive_pages()
    elif sys.argv[1] == 'pdfs':
        archive_pdfs()
