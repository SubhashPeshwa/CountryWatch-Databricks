import os
import json
import requests

def call_data_gov_api(uri):
    """
    """

    url = "https://api.data.gov.in"
    api_key = env.config.api_key
    print("Test")
    resp_format = "json"
    limit = 500
    offset = 0
    resp = requests.get("{}{}?api-key={}&format={}&limit={}&offset={}".format(url,uri,api_key,resp_format,limit,offset))

    return resp
    
def call_data_gov_catalog(uri, field_sector):
    """
    """

    url = f"https://data.gov.in"
    limit = 5000
    offset = 0
    if field_sector != "":
        params = f"?offset={offset}&limit={limit}&sort[ogdp_view_count]=desc&filters[field_sector:name][]={field_sector}"
    else:
        params = f"?offset={offset}&limit={limit}&sort[ogdp_view_count]=desc"
        
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'Host': 'data.gov.in',
        'Referer': 'https://data.gov.in/catalogs'
    }
    url = f"{url}{uri}{params}"
    print(url)
    resp = requests.get(url, headers=headers)

    return resp

if __name__=="__main__":
    print(call_data_gov_catalog("/backend/dmspublic/v1/resources_noaggs", ""))