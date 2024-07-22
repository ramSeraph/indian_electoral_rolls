from pathlib import Path

import cv2
from imgcat import imgcat
from PIL import Image, ImageOps
import numpy as np
import pytesseract

tessdir = Path(__file__).parent / 'models'
olddir  = tessdir / 'old'
newdir  = tessdir / 'lstm'

def thresholding(img, lower, upper):
    bw = np.asarray(img).copy()
    bw[bw < lower] = 0    # Black
    bw[bw >= upper] = 255 # White
    return Image.fromarray(bw)

def median_blur_row(arr, r):
    h,w = arr.shape
    for c in range(w):
        above = arr[r-1,c]
        below = arr[r+1,c]
        if above == 0 or below == 0:
            arr[r,c] = 0
        else:
            arr[r,c] = 255

def median_blur_diag(arr):
    h,w = arr.shape
    c = 2
    step = 2
    r = h - 1
    while r > 0:
        for i in range(step):
            bcount = 0
            wcount = 0
            for r1, c1 in [(r-1,c),(r+1,c),(r,c),
                           (r-1,c-1),(r,c-1),(r+1,c-1),
                           (r-1,c+1),(r,c+1),(r+1,c+1)]:
                v = 255 if (r1 < 0 or c1 < 0 or r1 > h-1 or c1 > w-1) else arr[r1,c1]
                if v == 255:
                    wcount += 1
                else:
                    bcount += 1
            
            arr[r,c] = 0 if bcount > wcount else 255
            c += 1

        if step == 2:
            step = 3
        else:
            step = 2

        r -= 1


def join(split1, split2, h,w):
    scratch = np.zeros((h,w), dtype=np.uint8)
    arr1, loc1 = split1
    arr2, loc2 = split2
    h1,w1 = arr1.shape
    h2,w2 = arr2.shape
    x1,y1 = loc1
    x2,y2 = loc2
    
    scratch[y1:y1+h1,x1:x1+w1] = arr1
    scratch[y2:y2+h2,x2:x2+w2] = arr2


    x_min = min(x1,x2)
    y_min = min(y1,y2)
    x_max = max(x1+w1,x2+w2)
    y_max = max(y1+h1,y2+h2)

    return scratch[y_min:y_max, x_min:x_max].copy(), (x_min,y_min)



def join_dots(splits, ih,iw):
    out = []
    dots = []
    for img, loc in splits:
        h,w = img.shape
        area = h*w
        if area < 30:
            dots.append((img, loc))
        else:
            out.append((img,loc))

    for i in range(len(out)):
        img, loc = out[i]
        h,w = img.shape
        x,y = loc
        ul_x = x + w
        ul_y = y
        for dot in dots:
            d_img, d_loc = dot
            dh,dw = d_img.shape
            dx,dy = d_loc
            ll_x = dx + dw
            ll_y = dy + dh
            if abs(ll_x - ul_x) < 5 and ul_y - ll_y < 5:
                out[i] = join(out[i], dot, ih,iw) 

    return out
        

def split_img(arr):
    ih,iw = arr.shape
    splits = []
    analysis = cv2.connectedComponentsWithStats(arr, 4, cv2.CV_32S)
    (totalLabels, label_ids, values, centroid) = analysis
    #contours, _ = cv2.findContours(arr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for i in range(1, totalLabels): 
        area = values[i, cv2.CC_STAT_AREA] 
        if area < 5:
            continue
        x = values[i, cv2.CC_STAT_LEFT] 
        y = values[i, cv2.CC_STAT_TOP] 
        w = values[i, cv2.CC_STAT_WIDTH] 
        h = values[i, cv2.CC_STAT_HEIGHT] 
        crop_arr       =       arr[y:y+h, x:x+w].copy()
        crop_label_ids = label_ids[y:y+h, x:x+w].copy()
        crop_arr[crop_label_ids != i] = 0
        splits.append((crop_arr, (x,y)))
    splits.sort(key=lambda x:x[1][0])
    splits = join_dots(splits, ih, iw)
    return splits


def remove_lines(arr):
    median_blur_diag(arr)
    median_blur_row(arr, 30)
    median_blur_row(arr, 40)
    median_blur_row(arr, 50)

def invert(arr):
    arr[arr == 255] = 1
    arr[arr == 0] = 255
    arr[arr == 1] = 0

def dilate_and_erode(arr):
    kernel = np.ones((3, 3), np.uint8)
    arr = cv2.dilate(arr, kernel, iterations=1)
    arr = cv2.erode(arr, kernel, iterations=1)
    return arr

expected_chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
def clean_text(text, expect_single_char):
    text = text.strip().lower()
    out = ''
    for c in text:
        if c in expected_chars:
            out += c
    if out == '':
        return out
    if expect_single_char:
        return out[0]
    return out
    
def correct_text(txt, area, ch, cw):
    if txt == '0' and area < 500:
        return 'o'

    #p -> 33,28 i -> 32,11 1 => 33,17, q => 33,22
    if txt == 'i':
        if cw > 13:
            if cw < 20:
                return '1'
            elif cw < 25:
                return 'q'
            else:
                return 'p'

    if txt == 'f' and ch < 30:
        return 'r'

    if txt == 'l' and cw > 15:
        return '4'

    return txt

def solve_captcha(img):
    img = img.convert('L')

    img = thresholding(img, 128, 128)

    arr = np.asarray(img).copy()

    remove_lines(arr)
    invert(arr)
    arr = dilate_and_erode(arr)

    img = Image.fromarray(arr)
    #imgcat(img)


    splits = split_img(arr)

    text = ''
    for c_arr,loc in splits:
        x,y = loc
        ch,cw = c_arr.shape
        #print(loc, (x+cw,y+ch))
        #print(ch*cw, ch, cw)
        area = ch*cw
        expect_single_char = area < 1000
        
        expected_h = 32 if expect_single_char else 64
        if ch < expected_h:
            ratio = expected_h / ch
            c_arr = cv2.resize(c_arr, None, fx=ratio, fy=ratio, interpolation=cv2.INTER_CUBIC)


        c_img = Image.fromarray(c_arr)
        c_img = ImageOps.expand(c_img, border=4, fill='black')

        #imgcat(c_img)

        psm = 10 if expect_single_char else 8
        config_base = ' --oem {} --psm {} --tessdata-dir "{}" configfile myconfig'
        config_old = config_base.format(0, psm, str(olddir))
        config_new = config_base.format(1, psm, str(newdir))
        ctext = pytesseract.image_to_string(c_img, config=config_old)
        #print(ctext)
        ctext = clean_text(ctext, expect_single_char)
        ctext = correct_text(ctext, area, ch, cw)
        #print(ctext)
        text += ctext
    return text
