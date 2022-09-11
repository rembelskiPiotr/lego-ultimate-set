import requests
import pandas as pd
import time


def build_url(endpoint, API_KEY, page_size):
    base_url = 'https://rebrickable.com/api/v3/lego/'
    url = base_url + endpoint + '/?key=' + API_KEY + '&page_size=' + page_size
    return url


def get_df_from_api(endpoint, API_KEY, page_size):
    url = build_url(endpoint, API_KEY, page_size)
    response = requests.get(url).json()
    df = pd.json_normalize(response, 'results')
    next_url = response['next']
    while next_url:
        response = requests.get(next_url).json()
        df = pd.concat([df, pd.json_normalize(response, 'results')])
        next_url = response['next']
        time.sleep(1)
    return df