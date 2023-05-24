#SERP - Search Engine Results Page
from setup import *

text_search = 'mx master 3s'
num_of_pages_crawl = 5

url = 'https://tiki.vn/api/v2/products/188685608'
headers = ''
params = {}
start_time = time.time()

response = requests.get(url=url, headers=headers, params=params)
products_detail_json = response.json()
products_detail = []
products_detail.append({
    'product_id': products_detail_json.get('id'),
    'all_time_quantity_sold': products_detail_json.get('all_time_quantity_sold'),
    'category_id': products_detail_json.get('categories').get('id'),
    'category_name': products_detail_json.get('categories').get('name'),
    'configurable_products': products_detail_json.get('configurable_products')
    })

print(products_detail)
print("--- Runtime: {} seconds ---".format(round(time.time() - start_time), 0))





            