#SERP - Search Engine Results Page
from setup import *

df = pd.read_csv("E:\MyDesktop\ThaiTran\Personal_Project\Project_Tiki\data_backup\get_products_SERP.csv")
df['completion_time'] = pd.to_datetime(df['completion_time'])
result = df.to_dict(orient = "records")

def process_dictionary(record):
    existing_record = next((item for item in products_id if item['seller_product_id'] == record['seller_product_id']), None)
    
    if existing_record:
        if existing_record['completion_time'] > record['completion_time']:
            existing_record['completion_time'] = record['completion_time']
    else:
        products_id.append(record)

# Gọi hàm xử lý cho dictionary
products_id = []
process_dictionary(record)

# In list products_id sau khi xử lý
for product in products_id:
    print(product)

