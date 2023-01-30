from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from datetime import datetime, timedelta
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine

class ETL:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db_name, final_table_name):
        self.url_core='https://www.olx.pl'
        self.str_today_date = datetime.today().strftime('%Y-%m-%d')

        self.user = mysql_user
        self.password = mysql_password
        self.port = mysql_port
        self.host = mysql_host
        self.db_name = mysql_db_name

        self.engine = create_engine(f'mysql+mysqldb://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}')
        self.mysql_connection = self.engine.connect()
        self.final_table_name = final_table_name
    
    def close_db_connections(self):
        # CLose mysql connection
        self.mysql_connection.close()
        self.engine.dispose()

    def scrap_main_pages(self, max_main_page=25, delay=4):
        url_main = self.url_core + '/d/nieruchomosci/mieszkania/wynajem/?'
        url_filter_rooms = '&search[filter_enum_rooms][0]='
        url_filter_pages = 'page='
        url_filter_order = '&search[order]=created_at%3Adesc'
        list_number_of_rooms = ['one', 'two', 'three', 'four']
        list_variables = ['tytul',
                        'url',
                        'cena',
                        'powierzchnia',
                        'lokalizacja_i_datestamp',
                        'liczba_pokoi']
        dict_template = {key: None for key in list_variables}

        df = pd.DataFrame()
        hours_delay = datetime.now() - timedelta(hours = delay)
        str_hours_delay = hours_delay.strftime('%Y-%m-%d %H:%M:%S')

        for room in list_number_of_rooms:
            page_number = 1
            ads_counter = 1
            old_ads_counter = 0
            max_main_page = max_main_page
            
            try:
                while page_number <= max_main_page:
                    if page_number == 1:
                        url_to_check = url_main + url_filter_rooms + room + url_filter_order
                    else:
                        url_to_check = url_main + url_filter_pages + str(page_number) + url_filter_rooms + room + url_filter_order
                    
                    result = requests.get(url_to_check)
                    content = result.text
                    soup = BeautifulSoup(content, 'lxml')
                    ads = soup.find_all('a', class_ = 'css-rc5s2u')
                    
                    for ad in ads:
                        dict_temporary = dict_template

                        try:
                            if_today = ad.find('p', class_ = 'css-veheph er34gjf0').text
                        except:
                            continue

                        if if_today.find('Dzisiaj') != -1:

                            location_and_datestamp = ad.find('p', class_ = 'css-veheph er34gjf0').text.strip()
                            r = re.split(' - ', location_and_datestamp)
                            datestamp = r[1]
                            datestamp = self.str_today_date + ' ' + datestamp[-5:] + ':00'

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
                                    title = ad.find('h6', class_ = 'css-16v5mdi er34gjf0').text
                                    dict_temporary['tytul'] = title
                                except:
                                    pass

                                try:
                                    url = ad.get('href')
                                    dict_temporary['url'] = url
                                except:
                                    pass

                                try:
                                    price = ad.find('p', class_ = 'css-10b0gli er34gjf0').text 
                                    dict_temporary['cena'] = price            
                                except:
                                    pass

                                try:
                                    flat_area = ad.find('div', class_ = 'css-1kfqt7f').text 
                                    dict_temporary['powierzchnia'] = flat_area           
                                except:
                                    pass

                                try:
                                    location_and_datestamp = ad.find('p', class_ = 'css-veheph er34gjf0').text
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
        
        # return df

    def scrap_details_olx(self, df_input, column_name='url'):
        df = df_input

        i = 0
        while i < len(df.index):
            try:
                ad_link = df.at[i, column_name]

                if '/d/oferta/' in ad_link:
                    result = requests.get(f'{self.url_core}{ad_link}')
                    content = result.text
                    soup = BeautifulSoup(content, 'lxml')

                    try:
                        list_details = [element.get_text() for element in soup.find_all('p', class_ = 'css-b5m1rv er34gjf0')]

                        df.at[i, 'typ_ogloszenia'] = list_details[0]

                        r = re.compile('poziom', re.IGNORECASE)
                        floor = list(filter(r.search, list_details))[0]
                        df.at[i, 'pietro'] = floor

                        r = re.compile('umeblowane', re.IGNORECASE)
                        furniture = list(filter(r.search, list_details))[0]             
                        df.at[i, 'umeblowanie'] = furniture

                        r = re.compile('rodzaj zabudowy', re.IGNORECASE)
                        building_type = list(filter(r.search, list_details))[0]
                        df.at[i, 'rodzaj_zabudowy'] = building_type

                        r = re.compile('czynsz', re.IGNORECASE)
                        utilities = list(filter(r.search, list_details))[0]
                        df.at[i, 'oplaty_dodatkowe'] = utilities
                    except:
                        pass

                    i += 1
                
                else:
                    i += 1
                    pass

            except:       
                i += 1
                continue

        # return df
    
    def scrap_details_otodom(self, df_input, column_name='url'):
        options = Options()
        options.add_argument('--disable-notifications')
        options.add_argument('--headless') # Best not to use, not to have IP blocked by website but I don't have better solution for now
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-blink-features=AutomationControlled')

        driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)

        df = df_input

        i = 0
        while i < len(df.index):
            try:
                ad_link = df.at[i, column_name]

                if 'otodom' in ad_link:
                    driver.get(ad_link)
                    sleep(5)
                    
                    try:
                        for element in driver.find_elements(By.CLASS_NAME, 'css-1ccovha.estckra9'):
                            category = element.find_element(By.CLASS_NAME, 'css-1h52dri.estckra7').text
                            
                            if re.search('piętro', category, re.IGNORECASE):
                                floor = element.find_element(By.CLASS_NAME, 'css-1wi2w6s.estckra5').text
                                df.at[i, 'pietro'] = floor
                            
                            elif re.search('czynsz', category, re.IGNORECASE):
                                utilities = element.find_element(By.CLASS_NAME, 'css-1wi2w6s.estckra5').text
                                df.at[i, 'oplaty_dodatkowe'] = utilities
                            
                            elif re.search('rodzaj zabudowy', category, re.IGNORECASE):
                                building_type = element.find_element(By.CLASS_NAME, 'css-1wi2w6s.estckra5').text
                                df.at[i, 'rodzaj_zabudowy'] = building_type

                        for element in driver.find_elements(By.CLASS_NAME, 'css-f45csg.estckra9'):
                            category = element.find_element(By.CLASS_NAME, 'css-1h52dri.estckra7').text
                            
                            if re.search('typ ogłoszeniodawcy', category, re.IGNORECASE):
                                ad_type = element.find_element(By.CLASS_NAME, 'css-1wi2w6s.estckra5').text
                                df.at[i, 'typ_ogloszenia'] = ad_type
                            
                            elif re.search('wyposażenie', category, re.IGNORECASE):
                                furniture = element.find_element(By.CLASS_NAME, 'css-1wi2w6s.estckra5').text
                                df.at[i, 'umeblowanie'] = furniture
                    except:
                        pass
                    
                    i += 1
                
                else:
                    i += 1
                    pass

            except:       
                i += 1
                continue

        driver.quit()
        # return df

    def transform_data(self, df_input):
        df = df_input

        try:
            # Location and datestamp
            df_split = df.lokalizacja_i_datestamp.str.split(pat = ' - ', n = 1, expand = True)
            df_split.rename(columns = {0: 'lokalizacja', 1: 'datestamp'}, inplace = True)
            df = pd.concat([df, df_split], axis = 1)
        except:
            pass

        try:
            # City and district
            df_split = df.lokalizacja.str.split(pat = ',', expand = True)
            df_split.rename(columns = {0: 'miasto', 1: 'dzielnica'}, inplace = True)
            df = pd.concat([df, df_split], axis = 1)
            df.miasto = df.miasto.str.strip()
            df.dzielnica = df.dzielnica.str.strip()
        except:
            pass
        
        try:
            # Cleaning up 'price' variable
            df.cena = df.cena.replace({'\..*$': '', '\D': ''}, regex = True) # Remove (1) everything after comma, (2) remove everything what is not a digit
            df.cena = df.cena.str.strip()
            df.cena = df.cena.astype('float')
        except:
            pass

        try:
            # Cleaning up 'flat_area' variable
            df.powierzchnia = df.powierzchnia.replace({' m²': '', ',': '.'}, regex = True)
            df.powierzchnia = df.powierzchnia.str.strip()
            df.powierzchnia = df.powierzchnia.astype('float')
        except:
            pass

        try:
            # Cleaning up 'floor' variable
            df.pietro = df.pietro.replace({'/.': '', 'Poziom:': '', 'Parter': 0}, regex = True)
            df.pietro = df.pietro.str.strip()
        except:
            pass
        
        try:
            # Cleaning up 'furniture' variable
            df.umeblowanie = df.umeblowanie.replace('Umeblowane:', '', regex = True)
            df.umeblowanie = df.umeblowanie.str.strip()
            df.loc[df.umeblowanie.str.len() > 3, 'umeblowanie'] = 'Tak'
        except:
            pass

        try:
            # Cleaning up 'building_type' variable
            df.rodzaj_zabudowy = df.rodzaj_zabudowy.replace('Rodzaj zabudowy: ', '', regex = True)
            df.rodzaj_zabudowy = df.rodzaj_zabudowy.str.capitalize()
            df.rodzaj_zabudowy = df.rodzaj_zabudowy.str.strip()
        except:
            pass

        try:
            # Cleaning up 'utilities' variable
            df.oplaty_dodatkowe = df.oplaty_dodatkowe.replace({'\D': ''}, regex = True)
            df.oplaty_dodatkowe = df.oplaty_dodatkowe.str.strip()
            df.oplaty_dodatkowe = df.oplaty_dodatkowe.astype('float')
        except:
            pass

        try:
            # Cleaning up 'datestamp' variable
            df.datestamp = self.str_today_date + ' ' + df.datestamp.str[-5:] + ':00'
        except:
            pass

        try:
            # Drop temporary columns
            df = df.drop(columns = ['lokalizacja_i_datestamp', 'lokalizacja'])
        except:
            pass

        try:
            # Cleaning up 'url' variable
            df.loc[df.url.str.find('otodom') == -1, 'url'] = self.url_core + df.url
        except:
            pass

        str_now = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        df['update_datetime'] = str_now

        try:
            # Order variables
            df = df[[
                    'update_datetime',
                    'datestamp',
                    'tytul',
                    'miasto',
                    'dzielnica',
                    'cena',
                    'powierzchnia',
                    'liczba_pokoi',
                    'oplaty_dodatkowe',
                    'pietro',
                    'umeblowanie',
                    'rodzaj_zabudowy',
                    'url'
                    ]]
        except:
            pass
        
        return df
    
    def update_final_table(self, df_input):
        df = df_input

        # Load data to final table
        df.to_sql(name = self.final_table_name, if_exists = 'append', index = False, con = self.mysql_connection)


