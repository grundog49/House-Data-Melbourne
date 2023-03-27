###############################################################################
#                             Loan Calculator                                 #
###############################################################################

###############################################################################
#                             Import Packages                                 #
###############################################################################

import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import string
import pandas as pd
from collections import defaultdict as dd
import asyncio

###############################################################################
#                             Define Constants                                #
###############################################################################

TEST_URL = 'https://www.auhouseprices.com/sold/list/VIC/'
DATA_PATH = r'D:\Users\grund\OneDrive\Documents\Python Scripts\Automation Stuff\Property Investment Calculator\House Data'

###############################################################################
#                                 Define Timer                                #
###############################################################################

# Using time module
import time

# defining the class
class Timer:
	
    def __init__(self, func = time.perf_counter):
        self.elapsed = 0.0
        self._func = func
        self._start = None

    # starting the module
    def start(self):
        if self._start is not None:
            raise RuntimeError('Already started')
        self._start = self._func()

    # stopping the timer
    def stop(self):
        if self._start is None:
            raise RuntimeError('Not started')
        end = self._func()
        self.elapsed += end - self._start
        self._start = None

    # resetting the timer
    def reset(self):
        self.elapsed = 0.0

    def running(self):
        return self._start is not None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


###############################################################################
#                           Define Key Functions                              #
###############################################################################

def month_abb_to_number(month_abbrev):
    if month_abbrev == 'Jan':
        return '01'
    elif month_abbrev == 'Feb':
        return '02'
    elif month_abbrev == 'Mar':
        return '03'
    elif month_abbrev == 'Apr':
        return '04'
    elif month_abbrev == 'May':
        return '05'
    elif month_abbrev == 'Jun':
        return '06'
    elif month_abbrev == 'Jul':
        return '07'
    elif month_abbrev == 'Aug':
        return '08'
    elif month_abbrev == 'Sep':
        return '09'
    elif month_abbrev == 'Oct':
        return '10'
    elif month_abbrev == 'Nov':
        return '11'
    else:
        return '12'

def joiner(list):
    return " ".join(list)

def slicer(list, single = None, range=None):
    if single:
        return list[single]
    else:
        if range[1]:
            return list[range[0]:range[1]]
        else:
            return list[range[0]:]


def spec_check(spec):
    if len(spec) == 0:
        return 0
    else:
        return(spec[0][-1])

def get_house_dets(link, act = False):
    response = requests.get(link)
    soup = BeautifulSoup(response.text, "html.parser")
    return dets_scrape(soup)


def dets_scrape(soup):

    house_record = dd(list)

    # Get the price and date of sale of property

    price_html = soup.findAll("h5")
    price_pattern = re.compile(r'\$[0-9]{0,3},[0-9]{3}')
    date_pattern = re.compile(r'[0-9]{1,2} [A-Za-z]{3} [0-9]{4}')
    if re.findall(price_pattern,str(price_html[0])):
        price_str = re.findall(price_pattern,str(price_html[0]))[0]
        price = int(price_str.translate(str.maketrans('', '', string.punctuation)))
        house_record['price'] = price
    else:
        house_record['price'] = 0
    if re.findall(date_pattern,str(price_html[0])):
        raw_date = re.findall(date_pattern,str(price_html[0]))[0]
        date_comps = raw_date.split(' ')
        date = date_comps[-1]+'-'+month_abb_to_number(date_comps[1])+'-'+date_comps[0]
        house_record['date'] = date
    else:
        house_record['date'] = 'N/A'

    # Extract the number of bedrooms, bathrooms and car spaces

    details = soup.findAll("ul", {'class':'list-unstyled'})
    bath_pattern = re.compile(r'i-bath"></i>[0-9]')
    bed_pattern = re.compile(r'i-bed"></i><big>[0-9]')
    car_pattern = re.compile(r'i-car"></i>[0-9]')
    bath_spec = re.findall(bath_pattern,str(details[0]))
    bed_spec = re.findall(bed_pattern,str(details[0]))
    car_spec = re.findall(car_pattern,str(details[0]))
    house_record['bedrooms'] = spec_check(bed_spec)
    house_record['bathrooms'] = spec_check(bath_spec)
    house_record['car spaces'] = spec_check(car_spec)

    # Extract the property type

    prop_type_pattern = re.compile(r'>[a-zA-Z]+<')
    prop_type = re.findall(prop_type_pattern,str(details[0].findAll("span")[0]))
    prop_type = prop_type[0][:-1][1:]
    house_record['property type'] = prop_type

    # Extract the distance to CBD, land size and year

    dist_land_size = details[1]
    land_size_pattern = re.compile(r'[0-9]+ m2')
    dist_pattern = re.compile(r'[0-9]+.[0-9]+ km')
    if re.findall(land_size_pattern, str(dist_land_size)):
        land_size = int(re.findall(land_size_pattern, str(dist_land_size))[0][:-3])
        house_record['land size'] = land_size
    else:
        house_record['land size'] = 0
    dist = float(re.findall(dist_pattern, str(dist_land_size))[0][:-3])
    house_record['distance'] = dist

    # Extract the addresss

    address_html = soup.findAll('h2')
    address_pattern = re.compile(r'>.+<')
    address = re.findall(address_pattern,str(address_html[0]))[0][:-1][1:]
    house_record['address'] = address
    return house_record


###############################################################################
#                               Main Program                                  #
###############################################################################
'''
melb_house_data = []

# Define the dictionary to hold the data

house_data = dd(list)

# Load the listing of postcodes

postcodes = pd.read_excel('Postcodes.xlsx')

# Get suburb and postcode details

postcodes['postcode'] = postcodes['Postcode Data'].str.split(" ")
postcodes['suburb'] = postcodes['Postcode Data'].str.split(" ")
postcodes['postcode'] = postcodes['postcode'].apply(slicer, single=1, range=None)
postcodes['suburb'] = postcodes['suburb'].apply(slicer, single=None, range=[4,None])
postcodes['suburb'] = postcodes['suburb'].apply(joiner)
postcodes['url'] = TEST_URL+postcodes['postcode']+'/'+postcodes['suburb']+'/'

for i in range(postcodes.shape[0]):
    time = Timer()
    # Using the details of the suburb and postcode, extract the link HTML

    time.start()
    postcode = str(postcodes.iloc[i,0]).split(" ")[1]
    suburb = " ".join([str(item) for item in str(postcodes.iloc[i,0]).split(" ")[4:]])
    suburb_house_listings = requests.get(TEST_URL+postcode+'/'+suburb+'/')
    suburb_house_soup = BeautifulSoup(suburb_house_listings.text, "html.parser")

    # Get the number of properties sold during this time
    
    number_of_houses_pattern = re.compile(r'of [0-9]+')
    pattern_serach_results = re.findall(number_of_houses_pattern,suburb_house_listings.text)

    # Filter out all suburbs which had no houses sold based on the data

    if not pattern_serach_results:
        continue
    num_of_houses = int(re.findall(number_of_houses_pattern,suburb_house_listings.text)[0].split(" ")[-1])

    # For each page for a particular suburb extract the details of the property sale
    for i in range(1,(num_of_houses//12)+2):
        suburb_house_listings = requests.get(TEST_URL+postcode+'/'+suburb+'/'+str(i)+'/')
        suburb_house_soup = BeautifulSoup(suburb_house_listings.text, "html.parser")
        house_links = suburb_house_soup.findAll("a", {'class':'btn-more hover-effect'})
        link_pattern = re.compile(r'href=\".+\"')
        for links in house_links:
            record = get_house_dets(re.findall(link_pattern,str(links))[0][6:-1])
            house_data['price'].append(record['price'])
            house_data['date'].append(record['date'])
            house_data['bedrooms'].append(record['bedrooms'])
            house_data['bathrooms'].append(record['bathrooms'])
            house_data['car spaces'].append(record['car spaces'])
            house_data['property type'].append(record['property type'])
            house_data['land size'].append(record['land size'])
            house_data['address'].append(record['address'])
    suburb_df = pd.DataFrame.from_dict(house_data)
    suburb_df['suburb'] = suburb
    suburb_df['postcode'] = postcode
    time.stop()
    print(time.elapsed)
    print(suburb)

# Save data from each suburb

if len(melb_house_data) == 0:
    melb_house_data = suburb_df
else:
    melb_house_data = pd.concat([melb_house_data, suburb_df])

melb_house_data.to_csv('melb_house_data.csv')'''

###############################################################################
#                            Async Main Program                               #
###############################################################################

import asyncio
import aiofiles
import time
import aiohttp as aiohttp
from aiocsv import AsyncReader, AsyncDictReader, AsyncWriter, AsyncDictWriter
import csv
import json
import os

async def get_suburb_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            sub_tasks = []
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')
            number_of_houses_pattern = re.compile(r'of [0-9]+')
            pattern_serach_results = re.findall(number_of_houses_pattern,soup.text)
            if pattern_serach_results:
                num_of_houses = int(re.findall(number_of_houses_pattern,soup.text)[0].split(" ")[-1])
                pages = pd.DataFrame([i for i in range(1,(num_of_houses//12)+2)],columns=['page'])
                pages['url'] = url+pages['page'].astype(str)+'/'
                for i in range(pages.shape[0]):
                    sub_task = asyncio.create_task(get_house_page(pages.iloc[i,-1]))
                    sub_tasks.append(sub_task)
            await asyncio.gather(*sub_tasks)

async def get_house_dets(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')

            house_record = dd(list)

            # Get the price and date of sale of property

            price_html = soup.findAll("h5")
            price_pattern = re.compile(r'(\$[0-9]{0,3},[0-9]{3},[0-9]{3})|(\$[0-9]{0,3},[0-9]{3})')
            date_pattern = re.compile(r'[0-9]{1,2} [A-Za-z]{3} [0-9]{4}')
            if re.findall(price_pattern,str(price_html[0])):
                if re.findall(price_pattern,str(price_html[0]))[0][0]:
                    price_str = re.findall(price_pattern,str(price_html[0]))[0][0]
                    price = int(price_str.translate(str.maketrans('', '', string.punctuation)))
                elif re.findall(price_pattern,str(price_html[0]))[0][1]:
                    price_str = re.findall(price_pattern,str(price_html[0]))[0][1]
                    price = int(price_str.translate(str.maketrans('', '', string.punctuation)))
                house_record['price'] = price
            else:
                house_record['price'] = 0
            if re.findall(date_pattern,str(price_html[0])):
                raw_date = re.findall(date_pattern,str(price_html[0]))[0]
                date_comps = raw_date.split(' ')
                date = date_comps[-1]+'-'+month_abb_to_number(date_comps[1])+'-'+date_comps[0]
                house_record['date'] = date
            else:
                house_record['date'] = 'N/A'

            # Extract the number of bedrooms, bathrooms and car spaces

            details = soup.findAll("ul", {'class':'list-unstyled'})
            bath_pattern = re.compile(r'i-bath"></i>[0-9]')
            bed_pattern = re.compile(r'i-bed"></i><big>[0-9]')
            car_pattern = re.compile(r'i-car"></i>[0-9]')
            bath_spec = re.findall(bath_pattern,str(details[0]))
            bed_spec = re.findall(bed_pattern,str(details[0]))
            car_spec = re.findall(car_pattern,str(details[0]))
            house_record['bedrooms'] = spec_check(bed_spec)
            house_record['bathrooms'] = spec_check(bath_spec)
            house_record['car spaces'] = spec_check(car_spec)

            # Extract the property type

            prop_type_pattern = re.compile(r'>[a-zA-Z]+<')
            prop_type = re.findall(prop_type_pattern,str(details[0].findAll("span")[0]))
            prop_type = prop_type[0][:-1][1:]
            house_record['property type'] = prop_type

            # Extract the distance to CBD, land size and year

            dist_land_size = details[1]
            land_size_pattern = re.compile(r'[0-9]+ m2')
            dist_pattern = re.compile(r'[0-9]+.[0-9]+ km')
            if re.findall(land_size_pattern, str(dist_land_size)):
                land_size = int(re.findall(land_size_pattern, str(dist_land_size))[0][:-3])
                house_record['land size'] = land_size
            else:
                house_record['land size'] = 0
            if re.findall(dist_pattern, str(dist_land_size)):
                dist = float(re.findall(dist_pattern, str(dist_land_size))[0][:-3])
            else:
                dist = 0
            house_record['distance'] = dist

            # Extract the addresss

            address_html = soup.findAll('h2')
            address_pattern = re.compile(r'>.+<')
            address = re.findall(address_pattern,str(address_html[0]))[0][:-1][1:]
            house_record['address'] = address
            await save_house(house_record)

async def save_house(house_record):
    json_file_name = house_record['address'].replace(' ', '_').replace('/', '_').replace('\"', '_')
    i = 'I'
    while os.path.isfile(f'suburb_data/{json_file_name}.json'):
        json_file_name+=i
    with open(f'suburb_data/{json_file_name}.json', 'w') as house_file:
        json.dump(house_record, house_file)

async def get_house_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')
            house_links = soup.findAll("a", {'class':'btn-more hover-effect'})
            link_pattern = re.compile(r'href=\".+\"')
            sub_tasks = []
            for link in house_links:
                sub_task = asyncio.create_task(get_house_dets(re.findall(link_pattern,str(link))[0][6:-1]))
                sub_tasks.append(sub_task)
            await asyncio.gather(*sub_tasks)

            

async def main():
    start_time = time.time()

    # Load the listing of postcodes

    postcodes = pd.read_excel('Postcodes.xlsx')
    postcodes = postcodes.iloc[0:2]
    print(postcodes)

    # Get suburb and postcode details

    tasks = []
    postcodes['postcode'] = postcodes['Postcode Data'].str.split(" ")
    postcodes['suburb'] = postcodes['Postcode Data'].str.split(" ")
    postcodes['postcode'] = postcodes['postcode'].apply(slicer, single=1, range=None)
    postcodes['suburb'] = postcodes['suburb'].apply(slicer, single=None, range=[4,None])
    postcodes['suburb'] = postcodes['suburb'].apply(joiner)
    postcodes['url'] = TEST_URL+postcodes['postcode']+'/'+postcodes['suburb']+'/'
    for i in range(postcodes.shape[0]):
        task = asyncio.create_task(get_suburb_page(postcodes.iloc[i,-1]))
        tasks.append(task)

    print('Saving the output of extracted information')
    await asyncio.gather(*tasks)

    time_difference = time.time() - start_time
    print(f'Scraping time: %.2f seconds.' % time_difference)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())