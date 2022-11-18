from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from datetime import datetime, timedelta

class ETL:
    def __init__(self):
        self.url_core = 'https://www.olx.pl'
    
    def scrap_main_pages(self, max_main_page=25, delay=4):
        url_main = 'https://www.olx.pl/d/nieruchomosci/mieszkania/wynajem/?search%5Border%5D=created_at%3Adesc'
        url_filter_rooms = '&search%5Bfilter_enum_rooms%5D%5B0%5D='
        url_filter_pages = '&page='
        list_number_of_rooms = ['one', 'two', 'three', 'four']
        list_variables = ['tytul',
                        'url',
                        'cena',
                        'powierzchnia',
                        'lokalizacja_i_datestamp',
                        'liczba_pokoi']
        dict_template = {key: None for key in list_variables}

        df = pd.DataFrame()
        str_today_date = datetime.today().strftime('%Y-%m-%d')
        hours_delay = datetime.now() - timedelta(hours = delay)
        str_hours_delay = hours_delay.strftime('%Y-%m-%d %H:%M:%S')

        for room in list_number_of_rooms:
            page_number = 1
            ads_counter = 0
            old_ads_counter = 0
            max_main_page = max_main_page
            
            try:
                while page_number <= max_main_page:
                    url_to_check = url_main + url_filter_rooms + room + url_filter_pages + str(page_number)
                    result = requests.get(url_to_check)
                    content = result.text
                    soup = BeautifulSoup(content, 'lxml')
                    ads = soup.find_all('a', class_ = 'css-rc5s2u')
                    
                    for ad in ads:
                        dict_temporary = dict_template

                        try:
                            if_today = ad.find('p', class_ = 'css-p6wsjo-Text eu5v0x0').text
                        except:
                            continue

                        if if_today.find('Dzisiaj') != -1:

                            location_and_datestamp = ad.find('p', class_ = 'css-p6wsjo-Text eu5v0x0').text.strip()
                            r = re.split(' - ', location_and_datestamp)
                            datestamp = r[1]
                            datestamp = str_today_date + ' ' + datestamp[-5:] + ':00'

                            if datestamp >= str_hours_delay:

                                old_ads_counter = 0

                                if room == 'one':
                                    dict_temporary['liczba_pokoi'] = 1
                                elif room == 'two':
                                    dict_temporary['liczba_pokoi'] = 2
                                elif room == 'three':
                                    dict_temporary['liczba_pokoi'] = 3
                                elif room == 'four':
                                    dict_temporary['liczba_pokoi'] = '4 and more'

                                try:
                                    title = ad.find('h6', class_ = 'css-1pvd0aj-Text eu5v0x0').text
                                    dict_temporary['tytul'] = title
                                except:
                                    pass

                                try:
                                    url = ad.get('href')
                                    dict_temporary['url'] = url
                                except:
                                    pass

                                try:
                                    price = ad.find('p', class_ = 'css-1q7gvpp-Text eu5v0x0').text 
                                    dict_temporary['cena'] = price            
                                except:
                                    pass

                                try:
                                    flat_area = ad.find('div', class_ = 'css-1kfqt7f').text 
                                    dict_temporary['powierzchnia'] = flat_area           
                                except:
                                    pass

                                try:
                                    location_and_datestamp = ad.find('p', class_ = 'css-p6wsjo-Text eu5v0x0').text
                                    dict_temporary['lokalizacja_i_datestamp'] = location_and_datestamp
                                except:
                                    pass

                                df = pd.concat([df, pd.DataFrame.from_records([dict_temporary])], ignore_index = True)

                                ads_counter += 1

                            else:
                                old_ads_counter += 1

                        else:
                            old_ads_counter += 1

                        if old_ads_counter == 20:
                            raise StopIteration
                    
                    page_number += 1
            
            except StopIteration:
                pass
        
        return df