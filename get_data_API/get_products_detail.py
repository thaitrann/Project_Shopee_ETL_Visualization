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
    'day_ago_created': products_detail_json.get('day_ago_created'),
    'discount': products_detail_json.get('discount'),
    'discount_rate': products_detail_json.get('discount_rate'),
    'favourite_count': products_detail_json.get('favourite_count'),
    'gift_item_title': products_detail_json.get('gift_item_title'),
    'inventory_status': products_detail_json.get('inventory_status'),
    'inventory_type': products_detail_json.get('inventory_type'),
    'list_price': products_detail_json.get('list_price'),
    'quantity_sold': (products_detail_json.get('quantity_sold').get('value') \
        if products_detail_json.get('quantity_sold') else 0),
    'rating_average': products_detail_json.get('rating_average'),
    'review_count': products_detail_json.get('review_count'),
    'configurable_products': ([[item['child_id'], item['name'], item['option1'], \
        item['price'], item['inventory_status']] for item in products_detail_json.get('configurable_products')]\
            if products_detail_json.get('configurable_products') else [])
    })

print(products_detail)
print("--- Runtime: {} seconds ---".format(round(time.time() - start_time), 0))





            