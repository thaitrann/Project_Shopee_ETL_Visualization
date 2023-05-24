#SERP - Search Engine Results Page
from setup import *

text_search = 'mx master 3s'
num_of_pages_crawl = 5

collection_product_serp = products_tiki["collection_product_serp"]
url = 'https://tiki.vn/api/v2/products'
headers = ''
params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'trackity_id': '1277f557-319e-1bd7-b99a-5e5c7fc5069e',
    'sort': 'top_seller',
    'q': text_search,
    'page': 1
}
start_time = time.time()

def get_product_serp(url, headers, params, num_of_pages_crawl):
    products_id = []
    for i in range(1, num_of_pages_crawl + 1):
        params['page'] = i
        response = requests.get(url=url, headers=headers, params=params)
        if response.status_code == 200:
            print('Request success page {}!'.format(i))
            for record in response.json().get('data'):
                products_id.append({
                    'product_id': record.get('id'),
                    'product_name': record.get('name'),
                    'primary_category_name': record.get('primary_category_name'),
                    'brand_id': record.get('brand_id'),
                    'brand_name': record.get('brand_name'),
                    'discount': record.get('discount'),
                    'discount_rate': record.get('discount_rate'),
                    'original_price': record.get('original_price'),
                    'price': record.get('price'),
                    'primary_category_name': record.get('primary_category_name'),
                    'rating_average': record.get('rating_average'),
                    'review_count': record.get('review_count'),
                    'seller_id': record.get('seller_id'),
                    'seller_name': record.get('seller_name'),
                    'shipping_code': next(item for item in record.get('badges_new') if item["placement"] == "delivery_info")['code'],
                    'shipping_text': next(item for item in record.get('badges_new') if item["placement"] == "delivery_info")['text'],
                    'completion_time': datetime.now()
                    })
        time.sleep(random.randrange(1, 5))
        
    collection_product_serp.insert_many(products_id)
    print("--- Done! ---")
    
get_product_serp(url, headers, params, num_of_pages_crawl)
print("--- Runtime: {} seconds ---".format(round(time.time() - start_time), 0))




            