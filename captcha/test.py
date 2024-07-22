
from PIL import Image
from imgcat import imgcat
from annotate import get_truth, data_dir

from solve import solve_captcha


if __name__ == '__main__':
    truth = get_truth()
    total = len(truth)
    success = 0
    failure = 0
    count   = 0
    for k,v in truth.items():
        count += 1
        file = data_dir / f'{k}.png'
        img = Image.open(file)
        imgcat(img)
        val = solve_captcha(img)
        print(val)

        out = ''
        if v == val:
            success += 1
            out = 'SUCCESS'
        else:
            failure += 1
            out = 'FAILURE'
        print(f'{out}: {success}/{failure}/{total}') 

