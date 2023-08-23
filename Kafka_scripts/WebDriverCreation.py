
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import re
import time
import datetime
import os

class WebDriverCreation:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        chromedriver_path = 'path/to/chromedrive-wd/chromedriver.exe'
        os.environ['PATH'] = f"{chromedriver_path};{os.environ['PATH']}"
        self.wd = webdriver.Chrome(options=self.options)
       # self.wd = webdriver.Chrome(options=self.options)
