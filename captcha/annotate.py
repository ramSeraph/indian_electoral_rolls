import json
from pathlib import Path
from PIL import Image
from imgcat import imgcat

data_dir   = Path(__file__).parent / 'data'
truth_file = data_dir / 'truth.jsonl'

def get_truth():
    if not truth_file.exists():
        return {}
    truth = {}
    txt = truth_file.read_text()
    lines = txt.split('\n')
    for line in lines:
        line = line.strip()
        if line == '':
            continue
        arr = json.loads(line)
        k = arr[0]
        v = arr[1]
        truth[k] = v
    return truth

if __name__ == '__main__':
    data_dir.mkdir(exist_ok=True, parents=True)
    truth = get_truth()
    
    with open(truth_file, 'a') as f:
        files = data_dir.glob('*.png')
        for file in files:
            k = file.name[:-4]
            if k in truth:
                continue
            imgcat(Image.open(file))
            v = input('Enter Value: ')
            v = v.strip()
            if v == '':
                continue
    
            f.write(json.dumps([k,v]))
            f.write('\n')


