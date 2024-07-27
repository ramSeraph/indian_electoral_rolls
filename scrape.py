import io
import json
import time
import base64
from pathlib import Path
from pprint import pprint

import requests
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from imgcat import imgcat
from PIL import Image

from captcha.solve import solve_captcha

base_url     = 'https://voters.eci.gov.in/download-eroll'
api_base_url = 'https://gateway-voters.eci.gov.in/api/v1'

captcha_url    = api_base_url + '/captcha-service/generateCaptcha/EROLL'
state_list_url = api_base_url + '/common/states/'
lang_url       = api_base_url + '/printing-publish/get-ac-languages'
part_list_url  = api_base_url + '/printing-publish/get-part-list'
roll_url       = api_base_url + '/printing-publish/generate-published-geroll'
const_list_url_tpl = api_base_url + '/common/constituencies?stateCode={}'
district_url_tpl   = api_base_url + '/common/districts/{}'


data_dir = Path('data')
raw_dir = data_dir / 'raw'
captcha_dir = Path('captcha/data')


max_attempts = 5
initial_delay = 100

try_count = 1
curr_delay = initial_delay

class RetriableException(Exception):
    pass

class DelayedRetriableException(Exception):
    pass

def raise_delayed_exception_if_needed(resp):
    if resp.status_code != 500:
        return
    try:
        data = resp.json()
        msg = f'message: {data.get("message", None)}'
    except JSONDecodeError:
        msg = resp.text
    raise DelayedRetriableException(msg)


def get_state_list(session):
    state_list_file = raw_dir / 'state_list.json'
    if state_list_file.exists():
        return json.loads(state_list_file.read_text())

    resp = session.get(state_list_url)
    if not resp.ok:
        raise Exception(f'Unable to get state list from {state_list_url}')

    resp_text = resp.text
    state_list_file.write_text(resp_text)

    return json.loads(resp_text)
    

def get_district_list(session, scode):
    state_dir = raw_dir / f'{scode}'
    state_dir.mkdir(exist_ok=True, parents=True)

    d_file = state_dir / 'district_list.json'
    if d_file.exists():
        return json.loads(d_file.read_text())

    dist_list_url = district_url_tpl.format(scode)

    resp = session.get(dist_list_url)
    if not resp.ok:
        raise_delayed_exception_if_needed(resp)
        raise Exception(f'Unable to get district list for {scode} from {dist_list_url}')

    resp_text = resp.text
    d_file.write_text(resp_text)

    return json.loads(resp_text)


def get_constituency_list(session, scode):
    state_dir = raw_dir / f'{scode}'
    state_dir.mkdir(exist_ok=True, parents=True)

    c_file = state_dir / 'constituency_list.json'
    if c_file.exists():
        return json.loads(c_file.read_text())

    const_list_url = const_list_url_tpl.format(scode)

    resp = session.get(const_list_url)
    if not resp.ok:
        raise_delayed_exception_if_needed(resp)
        raise Exception(f'Unable to get constituency list for {scode} from {const_list_url}')

    resp_text = resp.text
    c_file.write_text(resp_text)

    return json.loads(resp_text)
 

def get_constituency_langs(session, c_info):
    scode = c_info['stateCd']
    dcode = c_info['districtCd']
    acno  = c_info['asmblyNo']

    c_dir = raw_dir / f'{scode}' / f'{acno}'
    c_dir.mkdir(exist_ok=True, parents=True)

    lang_file = c_dir / 'langs.json'
    if lang_file.exists():
        return json.loads(lang_file.read_text())

    postdata = { 'acNumber': acno, 'districtCd': dcode, 'stateCd': scode }
    resp = session.post(lang_url, json=postdata)
    if not resp.ok:
        raise_delayed_exception_if_needed(resp)
        raise Exception(f'Unable to get language list for constituency {acno} of {scode} from {lang_url}')
    
    data = json.loads(resp.text)
    if data['status'] != 'Success':
        raise DelayedRetriableException(f'Unable to get language list for constituency {acno} of {scode} from {lang_url}, message: {data["message"]}, status: {data["status"]}')

    langs = data['payload']

    lang_file.write_text(json.dumps(langs))

    return langs


def get_constituency_parts(session, c_info):
    scode = c_info['stateCd']
    dcode = c_info['districtCd']
    acno  = c_info['asmblyNo']

    c_dir = raw_dir / f'{scode}' / f'{acno}'
    c_dir.mkdir(exist_ok=True, parents=True)

    parts_file = c_dir / 'parts.json'
    if parts_file.exists():
        return json.loads(parts_file.read_text())

    postdata = { 'acNumber': acno, 'districtCd': dcode, 'stateCd': scode }
    resp = session.post(part_list_url, json=postdata)
    if not resp.ok:
        raise_delayed_exception_if_needed(resp)
        raise Exception(f'Unable to get parts list for constituency {acno} of {scode} from {part_list_url}')
    
    data = json.loads(resp.text)
    if data['status'] != 'Success':
        raise DelayedRetriableException(f'Unable to get parts list for constituency {acno} of {scode} from {part_list_url}, message: {data["message"]}, status: {data["status"]}')

    parts = data['payload']

    parts_file.write_text(json.dumps(parts))

    return parts

def get_captcha(session):
    resp = session.get(captcha_url)
    if not resp.ok:
        raise_delayed_exception_if_needed(resp)
        print(resp.text, resp.status_code)
        raise Exception(f'Unable to get captcha at {captcha_url}')

    data = resp.json()
    if data['status'] != 'Success':
        raise DelayedRetriableException(f'Unable to get captcha at {captcha_url}, message: {data["message"]}')

    if data['captcha'] is None:
        raise DelayedRetriableException('Got empty captcha')

    img_bytes = base64.b64decode(data['captcha'])
    img = Image.open(io.BytesIO(img_bytes))
    return data['id'], img

def make_download_call(session, postdata):
    try:
        resp = session.post(roll_url, json=postdata)
    except RequestException as ex:
        raise DelayedRetriableException(str(ex))

    if not resp.ok:
        if resp.status_code == 400:
            data = resp.json()
            msg = data['message']
            if msg == 'Invalid Catpcha':
                raise RetriableException('Failed to solve captcha')
        raise_delayed_exception_if_needed(resp)
        print('\t\t\tWARNING: Failed request - ', resp.text)
        raise Exception(f'Unable to get roll for part {postdata} at {roll_url}')

    data = resp.json()
    if data['status'] != 'Success':
        raise DelayedRetriableException(f'Unable to get roll for part {postdata} at {roll_url}, message: {data["message"]}, status: {data["status"]}')

    return data



def download_part(session, lang, part):

    acno   = part['acNumber']
    partno = part['partNumber']
    scode  = part['stateCd']
    dcode  = part['districtCd']

    pdf_file = raw_dir / f'{scode}' / f'{acno}' / f'{lang}' / f'{partno}.pdf'
    if pdf_file.exists():
        sz = pdf_file.stat().st_size
        return sz != 0

    pdf_file.parent.mkdir(exist_ok=True, parents=True)

    while True:
        try:
            captcha_id, captcha_img = get_captcha(session)
            captcha_val = solve_captcha(captcha_img)
            if captcha_val == '':
                raise RetriableException('Could not solve captcha')

            postdata = {
                'acNumber'   : acno,
                'captcha'    : captcha_val,
                'captchaId'  : captcha_id,
                'districtCd' : dcode,
                'langCd'     : lang,
                'partNumber' : partno,
                'stateCd'    : scode,
            }

            data = make_download_call(session, postdata)
        except RetriableException as ex:
            print(f'\t\t\tWARNING: {ex}')
            continue

        if data['file'] is None:
            print(f'\t\t\tWARNING: voter roll not available')
            pdf_file.write_text('')
            return False
        print(f'\t\t\twriting file: {pdf_file}')
        content = base64.b64decode(data['file'])

        pdf_file.write_bytes(content)
        return True


def collect_captchas(session, count):

    captcha_dir.mkdir(exist_ok=True, parents=True)
    for i in range(count):
        captcha_id, captcha_img = get_captcha(session)
        cfile = captcha_dir / f'{captcha_id}.png'
        captcha_img.save(cfile)
    

def download():
    session = requests.session()
    retry = Retry(
        total=max_attempts,
        read=max_attempts,
        connect=max_attempts,
        backoff_factor=initial_delay,
        status_forcelist=set([500]),
    )
    session.mount('http://', HTTPAdapter(max_retries=retry))
    session.mount('https://', HTTPAdapter(max_retries=retry))

    resp = session.get(base_url)
    if not resp.ok:
        raise Exception(f'Unable to get main page at {base_url}')
    reset_delay()

    state_list = get_state_list(session)
    reset_delay()

    for state_info in state_list:
        scode = state_info['stateCd']
        sname = state_info['stateName']
        print(f'handling state: {sname}')
  
        district_list = get_district_list(session, scode)
        reset_delay()
        constituency_list = get_constituency_list(session, scode)
        reset_delay()
        for constituency_info in constituency_list:
            acname = constituency_info['asmblyName']
            print(f'\thandling constituency: {acname}')
            langs = get_constituency_langs(session, constituency_info)
            reset_delay()
            parts = get_constituency_parts(session, constituency_info)
            reset_delay()
            for lang in langs:
                for part in parts:
                    part_name = part['partName']
                    print(f'\t\thandling lang: {lang}, part: {part_name}')
                    success = download_part(session, lang, part)
                    reset_delay()
                    



def reset_delay():
    global try_count
    global curr_delay
    try_count = 1
    curr_delay = initial_delay


if __name__ == '__main__':
    raw_dir.mkdir(exist_ok=True, parents=True)

    while True:
        try:
            download()
            exit(0)
        except DelayedRetriableException as ex:
            print(f'WARNING: {ex}..')
            if try_count > max_attempts:
                raise Exception('Unable to retrieve data')
            print(f'WARNING: sleeping for {curr_delay} before attempting again')
            time.sleep(curr_delay)
            try_count += 1
            curr_delay *= 2
            continue


