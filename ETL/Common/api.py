import os
import json
import requests

def call_data_gov_api(uri):
    """
    """

    url = "https://api.data.gov.in"
    api_key = env.config.api_key
    resp_format = "json"
    limit = 500
    offset = 0
    resp = requests.get("{}{}?api-key={}&format={}&limit={}&offset={}".format(url,uri,api_key,resp_format,limit,offset))

    return resp
    
def call_data_gov_catalog(uri, field_sector = "Lok%20Sabha"):
    """
    """

    url = "https://data.gov.in"
    limit = 500
    offset = 0
    params = f"?offset={offset}&limit={limit}&filters[ogpl_module_domain_access]=4&sort[published_date]=desc&filters[field_sector:name][]={field_sector}&filters[ogpl_module_domain_name][]=data.gov.in"

    resp = requests.get(f"{url}{uri}{params}")

    return resp