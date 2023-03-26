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

###############################################################################
#                             Define Constants                                #
###############################################################################

TEST_URL = 'https://www.auhouseprices.com/sold/list/VIC/'
DATA_PATH = r'D:\Users\grund\OneDrive\Documents\Python Scripts\Automation Stuff\Property Investment Calculator\House Data'

###############################################################################
#                           Define Key Functions                              #
###############################################################################

def get_house_dets(link, act = False):
    if act:
        house_record = dd(list)
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")

        # Get the price of the property

        price_html = soup.findAll("h5")
        price_pattern = re.compile(r'\$[0-9]{0,3},[0-9]{3}')
        if re.findall(price_pattern,str(price_html[0])):
            price_str = re.findall(price_pattern,str(price_html[0]))[0]
            price = int(price_str.translate(str.maketrans('', '', string.punctuation)))
            house_record['price'] = price
        else:
            house_record['price'] = 0

        # Extract the number of bedrooms, bathrooms and car spaces

        details = soup.findAll("ul", {'class':'list-unstyled'})
        spec_pattern = re.compile(r'(>[0-9])|(g>[0-9])')
        house_specs = re.findall(spec_pattern,str(details[0]))
        specs = []
        for spec in house_specs:
            if spec[0]:
                specs.append(spec[0][-1])
            else:
                specs.append(spec[1][-1])
        house_record['bedrooms'] = specs[0]
        house_record['bathrooms'] = specs[1]
        if len(specs) == 3:
            house_record['car spaces'] = specs[2]

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

# Define the dictionary to hold the data

house_data = dd(list)

# Load the listing of postcodes

postcodes = pd.read_excel('Postcodes.xlsx')

# Loop over each combination of suburb and post code

for i in range(postcodes.shape[0]):

    # Using the details of the suburb and postcode, extract the link HTML

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
    print(num_of_houses)

    # For each page for a particular suburb extract the details of the property sale
    for i in range(1,(num_of_houses//12)+2):
        suburb_house_listings = requests.get(TEST_URL+postcode+'/'+suburb+'/'+str(i)+'/')
        suburb_house_soup = BeautifulSoup(suburb_house_listings.text, "html.parser")
        house_links = suburb_house_soup.findAll("a", {'class':'btn-more hover-effect'})
        link_pattern = re.compile(r'href=\".+\"')
        for links in house_links:
            record = get_house_dets(re.findall(link_pattern,str(links))[0][6:-1], act=True)
            house_data['price'].append(record['price'])
            house_data['bedrooms'].append(record['bedrooms'])
            house_data['bathrooms'].append(record['bathrooms'])
            house_data['property type'].append(record['property type'])
            house_data['land size'].append(record['land size'])
            house_data['address'].append(record['address'])

# Get the html from the url

