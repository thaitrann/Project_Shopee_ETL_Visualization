from setup import *

#connect mongodb
collection_product_serp = products_tiki["collection_product_serp"]
for x in collection_product_serp.find({},{ "_id": 0, "product_id": 1, "seller_id": 1 }).limit(1):
    print(x['product_id'], x['seller_id'], sep=" ")


    url_detail_products = 'https://tiki.vn/api/v2/products/{}'.format(x['product_id'])
    url_seller = 'https://tiki.vn/api/shopping/v2/widgets/seller?seller_id={}'.format(x['seller_id'])
    url_review = 'https://tiki.vn/api/v2/reviews?product_id={}&seller_id={}'.format(x['product_id'], x['seller_id'])

    headers = ''
    params = {}
    start_time = time.time()

    response_detail_products = requests.get(url=url_detail_products, headers=headers, params=params)
    products_detail_json = response_detail_products.json()
    print(response_detail_products.url)
    
    response_seller = requests.get(url=url_seller, headers=headers, params=params)
    seller_json = response_seller.json().get('data').get('seller')
    print(response_seller.url)
    
    response_review = requests.get(url=url_review, headers=headers, params=params)
    review_json = response_review.json()
    print(response_review.url)