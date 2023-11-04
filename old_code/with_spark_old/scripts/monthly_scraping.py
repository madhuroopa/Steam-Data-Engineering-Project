#Scrapes the monthly website visitor count, engagements
from selenium import webdriver
import time
import json
from datetime import date
from bs4 import BeautifulSoup

class MonthlyScraping:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome(options=self.options)
        self.today = date.today()
        self.monthly_visits_url = 'https://data.similarweb.com/api/v1/data?domain=store.steampowered.com'
        self.news_url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid="
    def get_monthly_visits(self):
        self.wd.get(self.monthly_visits_url)
        self.wd.implicitly_wait(10)
        data = self.wd.page_source
        soup = BeautifulSoup(data, 'html.parser')
        pre_tag = soup.find('pre')
        json_data = pre_tag.text if pre_tag else None
        print(len(json_data))
        
        if json_data:
            with open(f'../data/monthly_data/{self.today.strftime("%Y-%m-%d")}_similarweb_data.json', 'w') as json_file:
                json_file.write(json.dumps(json.loads(json_data), indent=4)) 
                json_file.close()
            print("Data saved successfully as JSON.")
        else:
            print("Failed to retrieve valid JSON data. Check the URL and API response.")  

if __name__ == '__main__':
    monthly_scraping = MonthlyScraping()
    monthly_scraping.get_monthly_visits()            