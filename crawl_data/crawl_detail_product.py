from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

#setup selenium
link = 'https://shopee.vn/Chu%E1%BB%99t-kh%C3%B4ng-d%C3%A2y-Logitech-MX-MASTER-3S-H%C3%A0ng-ch%C3%ADnh-h%C3%A3ng-i.311276229.19766393727?sp_atk=c5941fe2-93ef-45ea-98ed-65a938a8961f&xptdk=c5941fe2-93ef-45ea-98ed-65a938a8961f'
start_time = time()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_experimental_option("detach", True)

browser = webdriver.Chrome(executable_path="chromedriver.exe",chrome_options=chrome_options)
browser.set_window_position(-1000, 0)
browser.maximize_window()
browser.get(link)
browser.implicitly_wait(10)

title = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > \
        div.flex.flex-auto.RBf1cu > div > div._44qnta > span").text

avg_star = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div:nth-child(1) > div._1k47d8._046PXf").text

total_reviews = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div:nth-child(2) > div._1k47d8").text

total_sold = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div.flex.X5u-5c > div.flex.jgUbWJ > div.P3CdcB").text

price = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > \
    div.ndOSOO > div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > \
        div > div:nth-child(3) > div > div > div:nth-child(1) > div > div > div").text

supplement_list = browser.find_element(By.CLASS_NAME, "h-y3ij").text.split("\n")
free_ship = "Miễn phí vận chuyển"
insurance = "Bảo hiểm Thiết bị điện tử"

# print(title, avg_star, total_reviews, total_sold, price, sep="\n")
# print(supplement_list)

#get price variation
browser.implicitly_wait(10)
price_variation = browser.find_elements(By.CLASS_NAME, "product-variation")
for p in price_variation:
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
browser.close()


