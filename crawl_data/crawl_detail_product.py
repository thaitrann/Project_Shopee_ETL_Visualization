from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

#setup selenium
link = 'https://shopee.vn/Chu%E1%BB%99t-kh%C3%B4ng-d%C3%A2y-Bluetooth-Logitech-MX-Master-3s-%E2%80%93-Y%C3%AAn-t%C4%A9nh-8K-DPI-Cu%E1%BB%99n-si%C3%AAu-nhanh-s%E1%BA%A1c-USB-C-Win-Mac-i.52679373.21616681122?sp_atk=2f5d4ef7-8a48-4747-be23-728698ab8fa0&xptdk=2f5d4ef7-8a48-4747-be23-728698ab8fa0'
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

free_ship = browser.find_element(By.CSS_SELECTOR, "#main > div > div:nth-child(3) > div:nth-child(1) > div > \
    div > div.container > div.product-briefing.flex.card.s9-a-0 > div.flex.flex-auto.RBf1cu > div > \
        div.h-y3ij > div > div.flex.rY0UiC.lml8Go > div > div.mHANnI > div.WZTmVh > div").text

supplement_list = browser.find_element(By.CLASS_NAME, "h-y3ij").text.split("\n")

print(supplement_list)
insurance = "Bảo hiểm Thiết bị điện tử"

if insurance in supplement_list:
    print(True)
else:
    print(False)

# print(title, avg_star, total_reviews, total_sold, price, free_ship, sep="\n")
sleep(2)
browser.close()


