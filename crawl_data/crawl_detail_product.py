from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

#setup selenium
link = 'https://shopee.vn/B%E1%BB%99-b%C3%A0n-ph%C3%ADm-k%C3%A8m-chu%E1%BB%99t-gaming-c%C3%B3-d%C3%A2y-gi%E1%BA%A3-c%C6%A1-t%C3%ADch-h%E1%BB%A3p-b%E1%BA%ADt-t%E1%BA%AFt-ch%E1%BA%BF-%C4%91%E1%BB%99-%C4%91%C3%A8n-led-ti%E1%BB%87n-l%E1%BB%A3i-d%C3%A0nh-cho-game-th%E1%BB%A7-v%C4%83n-ph%C3%B2ng-i.93922606.1546899006?sp_atk=42a5fb69-e1fc-4c81-93fb-5b81bee559f6&xptdk=42a5fb69-e1fc-4c81-93fb-5b81bee559f6'
start_time = time()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_experimental_option("detach", True)

browser = webdriver.Chrome(executable_path="chromedriver.exe",chrome_options=chrome_options)
browser.set_window_position(-1000, 0)
browser.maximize_window()
browser.get(link)
browser.implicitly_wait(10)

#get overall product
product_title = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > \
        div.flex.flex-auto.RBf1cu > div > div._44qnta > span").text

product_avg_star = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div:nth-child(1) > div._1k47d8._046PXf").text

product_total_reviews = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div:nth-child(2) > div._1k47d8").text

product_total_sold = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div.flex.jgUbWJ > div.P3CdcB").text

product_price_range = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div:nth-child(3) > div > div > div:nth-child(1) > div > div > div").text

product_supplement_list = browser.find_element(By.CLASS_NAME, "h-y3ij").text.split("\n")

product_free_ship = "Miễn phí vận chuyển"
product_insurance = "Bảo hiểm Thiết bị điện tử"

print(product_title, product_avg_star, product_total_reviews, product_total_sold, product_price_range, "---", sep="\n")
print(product_supplement_list)

#get price variation
browser.implicitly_wait(10)
product_price_variation = browser.find_elements(By.CLASS_NAME, "product-variation")
for p in product_price_variation:
    p.click()
    sleep(0.5)
    name_variation = p.text
    price = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
        div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
            div > div:nth-child(3) > div > div > div:nth-child(1) > div > div > div").text
    if p.get_attribute("aria-disabled") == "true":
        print(name_variation, "Out Of Stock","---" , sep="\n")
    else:
        print(name_variation, price,"---" , sep="\n")

sleep(1)
#get product shop
shop_name = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.jwlMoy > div > div.VlDReK").text

shop_favourite_tag = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.jwlMoy > a > div.NP8R3A").text

shop_link = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div.ndOSOO > div > div.container > div.UwHWuz > \
        div.NLeTwo.page-product__shop > div.jwlMoy > div > div.Uwka-w > a").get_attribute("href")

shop_total_review = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > div:nth-child(1) > span").text

shop_total_product = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > a > span").text

shop_response_rate = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > div:nth-child(2) > span").text

shop_response_time = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > div:nth-child(5) > span").text

shop_participation_time = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) \
    > div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > div:nth-child(3) > span").text

shop_follower = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > \
    div:nth-child(1) > div > div > div.container > div.UwHWuz > div.NLeTwo.page-product__shop \
        > div.Po6c6I > div:nth-child(6) > span").text

print(shop_name, shop_favourite_tag, shop_link, shop_total_review, shop_total_product, \
    shop_response_rate, shop_response_time, shop_participation_time, shop_follower, "---", sep="\n")

sleep(1)
browser.close()