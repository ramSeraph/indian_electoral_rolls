from pathlib import Path
from utils import convert_to_pages

if __name__ == '__main__':
    for p in Path('data/raw/').glob('*/*/*/*.pdf'):
        print(f'converting file {p}')
        convert_to_pages(p)
