
from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd
import re
import time
import datetime

class WebDriverCreation:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome(options=self.options)
