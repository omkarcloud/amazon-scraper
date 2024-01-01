
from botasaurus import bt
from botasaurus.cache import DontCache
from botasaurus import cl
from time import sleep
from botasaurus import *
from .utils import   default_request_options
import requests

FAILED_DUE_TO_CREDITS_EXHAUSTED = "FAILED_DUE_TO_CREDITS_EXHAUSTED"
FAILED_DUE_TO_NOT_SUBSCRIBED = "FAILED_DUE_TO_NOT_SUBSCRIBED"
FAILED_DUE_TO_NO_KEY = "FAILED_DUE_TO_NO_KEY"
FAILED_DUE_TO_UNKNOWN_ERROR = "FAILED_DUE_TO_UNKNOWN_ERROR"

def update_credits():
    credits_used  = bt.LocalStorage.get_item("credits_used", 0)
    bt.LocalStorage.set_item("credits_used", credits_used + 1)

def do_request(data, retry_count=3):
    
    params = data["params"]


    link = params["link"]
    key = data["key"]
    # print(params)
    # print("link", link)
    if retry_count == 0:
        print(f"Failed to get data, after 3 retries")
        return {
                        "data":  None,
                        "error":FAILED_DUE_TO_UNKNOWN_ERROR, 
                    }

    

    headers = {
        "X-RapidAPI-Key": key,
    	"X-RapidAPI-Host": "amazon-product-scraper8.p.rapidapi.com"
    }

    
    response = requests.get(link, headers=headers)
    response_data = response.json()
    if response.status_code == 200 or response.status_code == 404:
        
        message = response_data.get("message", "")
        if "API doesn't exists" in message:
            return {
                        "data":  None,
                        "error":FAILED_DUE_TO_UNKNOWN_ERROR
                    }

        update_credits()
        # print(response_data)
        # bt.write_json(response_data, "response.json")
        if response.status_code  == 404:
            print(f"No data found")
            
            return {
                "data": response_data,
                "error": None
            }

        return {
            "data": response_data,
            "error": None
        }
    else:

        message = response_data.get("message", "")
        
        if "exceeded the MONTHLY quota" in message:
            return  {
                        "data":  None,
                        "error":FAILED_DUE_TO_CREDITS_EXHAUSTED
                    }
        elif "exceeded the rate limit per second for your plan" in message or "many requests" in message:
            sleep(2)
            return do_request(data, retry_count - 1)
        elif "You are not subscribed to this API." in message:
            return {
                        "data": None,
                        "error": FAILED_DUE_TO_NOT_SUBSCRIBED
                    }

        print(f"Error: {response.status_code}", response_data)
        
        return  {
                        "data":  None,
                        "error":FAILED_DUE_TO_UNKNOWN_ERROR, 
                    }

@request(**default_request_options)
def search(_, data, metadata):
    if not metadata.get('key'):
         return  DontCache({
                        "data":  None,
                        "error":FAILED_DUE_TO_NO_KEY
                    })
    max_items = data['max']
    url = "https://amazon-product-scraper8.p.rapidapi.com/search/"
    qp = {"query": data['query']}
    params = {**qp, 'link':cl.join_link(url, query_params=qp)}

    request_data = {**metadata, "params": params}
    result = do_request(request_data)
    initial_results = cl.select(result, 'data', 'results', default=[])
    
    if not cl.select(result, 'error'):
        more_results = cl.select(result, 'data', 'results', default=[])
        print(f"Got {len(more_results)} more results")

    while cl.select(result, 'data', 'next') and (max_items is None or len(initial_results) < max_items):
        next = cl.select(result, 'data', 'next')

        params = {**qp, 'link':next}
        request_data = {**metadata, "params": params}
        result = do_request(request_data)
        if result.get('error'):
            break
        more_results = cl.select(result, 'data', 'results', default=[])
        print(f"Got {len(more_results)} more results")
        initial_results.extend(more_results)


    if cl.select(result, 'error'):
        return DontCache(result)
    else: 
        if max_items is not None:
            initial_results = initial_results[:max_items]

        result['data']['results'] = initial_results
        return result


@request(**default_request_options)
def get_product(_, data, metadata):
    if not metadata.get('key'):
         return  DontCache({
                        "data":  None,
                        "error":FAILED_DUE_TO_NO_KEY
                    })
    
    url = "https://amazon-product-scraper8.p.rapidapi.com/product/"
    qp = {"asin": data['asin']}
    params = {**qp, 'link':cl.join_link(url, query_params=qp)}

    request_data = {**metadata, "params": params}
    result = do_request(request_data)
    return result

