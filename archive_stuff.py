import json
from pathlib import Path
from utils import create_archive, upload_archive_to_r2

raw_dir = Path('data/raw')

for ldir in raw_dir.glob('*/*/*/'):
    if not ldir.is_dir():
        continue
    list_file = ldir.parent.parent / 'constituency_list.json'
    
    parts_file = ldir.parent / 'parts.json'
    parts = json.loads(parts_file.read_text())
    part_files = [ ldir / f'{p["partNumber"].pdf}' for p in parts ]
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

    print(ldir)
    create_archive(curr_cinfo, ldir.name)
    upload_archive_to_r2(curr_cinfo, ldir.name)
