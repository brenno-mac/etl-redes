import requests
import json

def getCreds():
    creds = dict()
    creds['access_token'] = 'EAARcGZBCPaSEBAK5D9HCSVTzZBpEkRo7qqXIr1xMGE4krx2uiWb1fh0abHBZB0b0c3aZCp2TZA9sMJNxZC7ZCuu9gBtLCA0LVD1O3wPDokxuNYUpl0xxfL3KPaND2Y33wm970CTcqKyi0EZCb798ZCySgD0WfCnWri4qhZCvWum319ZAQZDZD'
    creds['client_id'] = '1227174708209953'
    creds['client_secret'] = 'c0291a107c97315a44842ea85ca2913b'
    creds['graph_domain'] = 'https://graph.facebook.com'
    creds['graph_version'] = 'v12.0'
    creds['endpoint_base'] = creds['graph_domain'] + creds['graph_version'] + '/'
    creds['debug'] = 'no'
    
    return creds

def makeApiCall(url, endpointParams, debug = 'no'):
    data = requests.get(url, endpointParams)
    
    response = dict()
    response['url'] = url
    response['endpoint_params'] = endpointParams
    response['endpoint_params_pretty'] = json.dumps(endpointParams, indent=4)
    response['json_data'] = json.loads(data.content)
    response['json_data_pretty'] = json.dumps(response['json_data'], indent=4)
    
    if('yes' == debug):
        displayApiCallData(response)
        
    return response
    
def displayApiCallData(response):
    print ("\nURL: ")
    print (response['url'])
    print ("\nEndpoint Params:")
    print (response['endpoint_params_pretty'])
    print ("\nResponse: ")
    print (response['endpoint_params_pretty'])