import requests
class price:
    def __init__(self):
        self.prices_url='https://store.steampowered.com/api/appdetails?appids='

    def get_pricedetails(self,app_id):
        params= {
            'cc':'us',
            'filters':'price_overview'
        }
        self.response = requests.get(url = self.prices_url+app_id,params = params).json()
        return self.response
if __name__ == "__main__":
    obj = price()
    p=obj.get_pricedetails('1086940')
    pric=p['1086940']['data']['price_overview']['final_formatted']
    print(pric)
