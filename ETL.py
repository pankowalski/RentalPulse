from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from datetime import datetime, timedelta

class ETL:
    def __init__(self, url_core='https://www.olx.pl'):
        self.url_core = url_core
    
    def scrap_main_pages(self, max_main_page=25, delay=4):
        url_main = self.url_core + '/d/nieruchomosci/mieszkania/wynajem/?search%5Border%5D=created_at%3Adesc'
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

    def scrap_details_olx(self, df, column_name='url'):
        df = df

        i = 0
        while i < len(df.index):
            try:
                ad_link = df.at[i, column_name]

                if '/d/oferta/' in ad_link:
                    result = requests.get(f'{self.url_core}{ad_link}') # Ask for url access
                    content = result.text # Get HTML text page
                    soup = BeautifulSoup(content, 'lxml') # Convert to soup format for further proces

                    try:
                        list_details = [element.get_text() for element in soup.find_all('p', class_ = 'css-xl6fe0-Text eu5v0x0')]

                        df.at[i, 'typ_ogloszenia'] = list_details[0]

                        r = re.compile('poziom', re.IGNORECASE)
                        pietro = list(filter(r.search, list_details))[0]
                        df.at[i, 'pietro'] = pietro

                        r = re.compile('umeblowane', re.IGNORECASE)
                        umeblowanie = list(filter(r.search, list_details))[0]             
                        df.at[i, 'umeblowanie'] = umeblowanie

                        r = re.compile('rodzaj zabudowy', re.IGNORECASE)
                        rodzaj_zabudowy = list(filter(r.search, list_details))[0]
                        df.at[i, 'rodzaj_zabudowy'] = rodzaj_zabudowy

                        r = re.compile('czynsz', re.IGNORECASE)
                        oplaty_dodatkowe = list(filter(r.search, list_details))[0]
                        df.at[i, 'oplaty_dodatkowe'] = oplaty_dodatkowe
                    except:
                        pass

                    i += 1
                
                else:
                    i += 1
                    pass

            except:       
                i += 1
                continue

        return df

df = ETL().scrap_main_pages()
