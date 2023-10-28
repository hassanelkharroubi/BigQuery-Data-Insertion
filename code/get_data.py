import os
import json
import requests




def get_api():
    base_url = 'https://alliance-platform-virtualkey-prod.apps.eu.kamereon.io/v1/vkecus/logs'
    token_url = 'https://prod.eu.auth.kamereon.org/kauth/oauth2/k-external-prod/access_token'
    token_response = requests.post(
    token_url,
     data={
    "grant_type": "client_credentials",
    "client_id": "k-external-rsl-kvk-read-prod",
    "client_secret": "JVXdGAwSBKmv3Fc0rKbBPONDNWJkNyT64CduQgZPtEJlDBqy9aU1lvisCJf_STcI"
    })
    token_data = token_response.json()
    access_token = token_data["access_token"]
    data = token_response.json()
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    return base_url,headers


def get_data(filters):
    data_to_insert=[]
    print('getting API CREED ...')
    base_url,headers=get_api()
    print('base url : ',base_url)
    for filter_type in filters:
        url = f'{base_url}?filter%5Btype%5D={filter_type}'
        response = requests.get(url, headers=headers)
        data = response.json()
        log_records = data.get("data", [])
        for record in log_records:
            attributes = record.get("attributes", {})
            creation_date = attributes.get("creationDate")
            collection_date = attributes.get("collectionDate")
            producer = attributes.get("producer")
            vkecu_id = attributes.get("vkecuId")
            log_type = attributes.get("type")
            parameters = attributes.get("parameters", {})
            row = (creation_date, collection_date, producer, vkecu_id, log_type,parameters)
            data_to_insert.append(row)
    return data_to_insert

if __name__ == '__main__':
    filters = [
    "vkEcuMgt",
    #"popTokenReques",
    #"SpCmdSend",
    #"SpCmdRsp",
    #"SpCmdFail",
    #"SpNewRightError",
    #"SpNewRightGranted"
    ]
    data=get_data(filters)
    print(data)