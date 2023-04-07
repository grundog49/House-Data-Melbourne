###############################################################################
#                             Loan Calculator                                 #
###############################################################################

###############################################################################
#                             Import Packages                                 #
###############################################################################

import re
import string
import pandas as pd
from bs4 import BeautifulSoup
import asyncio

###############################################################################
#                             Define Constants                                #
###############################################################################

TEST_URL = 'https://www.auhouseprices.com/sold/list/VIC/'
DATA_PATH = r'D:\Users\grund\OneDrive\Documents\Python Scripts\Automation Stuff\Property Investment Calculator\House Data'

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

###############################################################################
#                            Async Main Program                               #
###############################################################################

import asyncio
import time
import aiohttp as aiohttp
import json
import os

async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*(sem_coro(c) for c in coros))

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
            await gather_with_concurrency(1,*sub_tasks)

async def save_link(link):
    json_file_name = link
    for punctuation in string.punctuation:
        json_file_name = json_file_name.replace(punctuation, '')
    link_dict = {'link': link}
    if not os.path.isfile(f'url_data/{json_file_name}.json'):
        with open(f'url_data/{json_file_name}.json', 'w') as house_file:
            json.dump(link_dict, house_file)

async def get_house_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            body = await resp.text()
            soup = BeautifulSoup(body, 'html.parser')
            house_links = soup.findAll("a", {'class':'btn-more hover-effect'})
            link_pattern = re.compile(r'href=\".+\"')
            sub_tasks = []
            for link in house_links:
                sub_task = asyncio.create_task(save_link(re.findall(link_pattern,str(link))[0][6:-1]))
                sub_tasks.append(sub_task)
            await gather_with_concurrency(12,*sub_tasks)

        
async def main():
    start_time = time.time()

    # Load the listing of postcodes

    postcodes = pd.read_excel('Postcodes.xlsx')

    # Get suburb and postcode details

    tasks = []
    postcodes['postcode'] = postcodes['Postcode Data'].str.split(" ")
    postcodes['suburb'] = postcodes['Postcode Data'].str.split(" ")
    postcodes['postcode'] = postcodes['postcode'].apply(slicer, single=1, range=None)
    postcodes['suburb'] = postcodes['suburb'].apply(slicer, single=None, range=[4,None])
    postcodes['suburb'] = postcodes['suburb'].apply(joiner)
    postcodes['url'] = TEST_URL+postcodes['postcode']+'/'+postcodes['suburb']+'/'
    for i in range(postcodes.shape[0]):
        await get_suburb_page(postcodes.iloc[i,-1])

    print('Saving the output of extracted information')

    time_difference = time.time() - start_time
    print(f'Scraping time: %.2f seconds.' % time_difference)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
