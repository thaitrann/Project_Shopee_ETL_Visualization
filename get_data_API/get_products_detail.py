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
    
    response_seller = requests.get(url=url_seller, headers=headers, params=params)
    seller_json = response_seller.json().get('data').get('seller')
    
    response_review = requests.get(url=url_review, headers=headers, params=params)
    review_json = response_review.json()

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
        'url': products_detail_json.get('short_url'),
        'configurable_products': ([[item['child_id'], item['name'], item['option1'], \
            item['price'], item['inventory_status']] for item in products_detail_json.get('configurable_products')]\
                if products_detail_json.get('configurable_products') else []),
        
        'seller_id': products_detail_json.get('current_seller').get('id'),
        'seller_name': products_detail_json.get('current_seller').get('name'),
        'seller_level': seller_json.get('store_level'),
        
        '1_star_count': response_review.json().get('stars').get('1').get('count'),
        '1_star_percent': response_review.json().get('stars').get('1').get('percent'),
        '2_star_count': response_review.json().get('stars').get('2').get('count'),
        '2_star_percent': response_review.json().get('stars').get('2').get('percent'),
        '3_star_count': response_review.json().get('stars').get('3').get('count'),
        '3_star_percent': response_review.json().get('stars').get('3').get('percent'),
        '4_star_count': response_review.json().get('stars').get('4').get('count'),
        '4_star_percent': response_review.json().get('stars').get('4').get('percent'),
        '5_star_count': response_review.json().get('stars').get('5').get('count'),
        '5_star_percent': response_review.json().get('stars').get('5').get('percent'),
        'completion_time': datetime.now()
        })


print(pd.DataFrame(products_detail))
pd.DataFrame(products_detail).to_csv("test.csv")
print("--- Runtime: {} seconds ---".format(round(time.time() - start_time), 0))

# task ngày mai:
#     hoàn thành lấy toàn detail product dựa trên product id và seller id trong database và lưu vào mongodb
#     lưu 2 file csv backup cho 2 bảng data
#     tìm hiểu spark để lưu trữ vào hadoop


            