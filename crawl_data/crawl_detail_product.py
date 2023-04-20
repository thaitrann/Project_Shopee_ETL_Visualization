from time import sleep, time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

#setup selenium
link = 'https://shopee.vn/H%E1%BB%99p-%C4%90%E1%BB%B1ng-B%E1%BA%A3o-V%E1%BB%87-Chu%E1%BB%99t-Logitech-MX-Master-2S-3-3S-Lucas-V%C3%B2m-i.88679925.6548500114?sp_atk=ff96a351-f2ec-4b4b-a3ca-dcebb3f99960&xptdk=ff96a351-f2ec-4b4b-a3ca-dcebb3f99960'
start_time = time()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_experimental_option("detach", True)

browser = webdriver.Chrome(executable_path="chromedriver.exe",chrome_options=chrome_options)
browser.set_window_position(-1000, 0)
browser.maximize_window()
browser.get(link)
browser.implicitly_wait(10)