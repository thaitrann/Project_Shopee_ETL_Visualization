from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_experimental_option("detach", True)

browser = webdriver.Chrome(executable_path="chromedriver.exe",chrome_options=chrome_options)
browser.set_window_position(-1000, 0)
browser.maximize_window()

def get_proxy(browser):
    browser.get('https://free-proxy-list.net/')
    table = browser.find_elements(By.CSS_SELECTOR,"table.table-striped.table-bordered tbody tr")
    proxy_list = []
    for row in table:
        proxy_list.append(row.text)
    print(proxy_list)

if __name__ == '__main__':
    get_proxy(browser)
    
#42.118.46.60 