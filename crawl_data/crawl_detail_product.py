from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

#setup selenium
link = 'https://shopee.vn/-M%C3%A3-ELBMO2-gi%E1%BA%A3m-12-%C4%91%C6%A1n-500K-Ugreen-Chu%E1%BB%99t-kh%C3%B4ng-d%C3%A2y-ti%E1%BB%87n-d%E1%BB%A5ng-6-n%C3%BAt-im-l%E1%BA%B7ng-5-c%E1%BA%A5p-%C4%91%E1%BB%99-4000dpi-cho-for-PC-MacBook-Air-M1-iPad-Gen-9-Laptop-i.325696535.14478688162?sp_atk=610cc335-0fbb-49d1-9ff7-6587423921a7&xptdk=610cc335-0fbb-49d1-9ff7-6587423921a7'
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


print(title, avg_star, total_reviews, total_sold, price, free_ship, sep="\n")

