import requests
import time
import pandas as pd
import random
from datetime import datetime
import pymongo

connection_string = "mongodb://localhost:27017"
myclient = pymongo.MongoClient(connection_string)
products_tiki = myclient["products_tiki"]

collections = ['collection_products_serp', 'collection_products_detail']

start_time = time.time()