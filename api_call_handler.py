import requests
import pandas as pd


def get_df_from_api(endpoint, API_KEY, page_size):

    url = 'https://rebrickable.com/api/v3/lego/' + endpoint + \
        '/?key=' + API_KEY + '&page_size=' + page_size
    response = requests.get(url)
    results = response.json()['results']
    df = pd.json_normalize(results)
    return df
