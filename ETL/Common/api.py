import os
import json
import requests

def call_data_gov_api(uri):
    """
    """

    url = "https://api.data.gov.in"
    # api_key = env.config.api_key
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
        params = f"?offset={offset}&limit={limit}&filters[ogpl_module_domain_access]=4&sort[published_date]=desc&filters[field_sector:name][]={field_sector}&filters[ogpl_module_domain_name][]=data.gov.in"
    else:
        params = f"?offset={offset}&limit={limit}&filters[ogpl_module_domain_access]=4&sort[published_date]=desc&filters[ogpl_module_domain_name][]=data.gov.in"
        
    headers = {
        'User-Agent': 'PostmanRuntime/7.32.2',
        'Host': 'data.gov.in',
        'Referer': 'https://data.gov.in/catalogs'
    }
    url = f"{url}{uri}{params}"

    resp = requests.get(url, headers=headers)

    return resp

# if __name__=="__main__":
#     print(call_data_gov_catalog("/backend/dmspublic/v1/catalogs", ""))