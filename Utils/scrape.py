import requests
import json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
# Marking start time for timing the script.
start_time = time.time()
# Global variables
house_details = []
SCRAPED_URLS = set()
raw_data = []
ERROR_COUNT = 0
URL_COUNT = 0
COUNTER = 0
COUNTER_LOCK = Lock()
# Acts as filter for the dictionary, we can add or remove (un)wanted data from the filtered dictionary
selected_values = [
    ("id", "id"),
    ("Street", "property.location.street"),
    ("Housenumber", "property.location.number"),
    ("Box", "property.location.box"),
    ("Floor", "property.location.floor"),
    ("City", "property.location.locality"),
    ("Postalcode", "property.location.postalCode"),
    ("Property type", "property.location.type"),
    ("Region", "property.location.regionCode"),
    ("District", "property.location.district"),
    ("Province", "property.location.province"),
    ("Subtype", "property.subtype"),
    ("Price", "price.mainValue"),
    ("Type of sale", "price.type"),
    ("Construction year", "property.building.constructionYear"),
    ("Bedroom Count", "property.bedroomCount"),
    ("Habitable surface", "property.netHabitableSurface"),
    ("Kitchen type", "property.kitchen.type"),
    ("Furnished", "transaction.sale.isFurnished"),
    ("Fireplace", "property.fireplaceExists"),
    ("Terrace", "property.hasTerrace"),
    ("Garden", "property.hasGarden"),
    ("Garden surface", "property.land.surface"),
    ("Facades", "property.building.facadeCount"),
    ("SwimmingPool", "property.hasSwimmingPool"),
    ("Condition", "property.building.condition"),
    ("EPC score", "transaction.certificates.epcScore"),
    ("Latitude", "property.location.latitude"),
    ("Longitude", "property.location.longitude"),
    ("Property url", "url")
]
#Start a Sesson as session
session = requests.Session()

def get_property(url, session):
    """
    Fetches the property details from the given URL.

    Args:
        url (str): The URL of the property.
        session (requests.Session): The session object to use for making the GET request.

    Returns:
        dict: The property details as a dictionary.
    """
    try:
        response = session.get(url)
        html_content = response.text
        start_marker = "window.classified = " #create variable for cutting the content string at the start of the dictionary
        end_marker = ";\n"                    #create variable for cutting off the end of the string
        start_index = html_content.find(start_marker) + len(start_marker) 
        end_index = html_content.find(end_marker, start_index)
        if start_index != -1 and end_index != -1: #check if we are not out of bounds with the string
            json_data = html_content[start_index:end_index] #create the dictionary from the resulting string {}
            house_dict = json.loads(json_data) #seperate dict for the filtered result
            house_dict["url"] = url  # Add the URL to the dictionary
            return house_dict, json_data
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"Error occurred during scraping: {e}")
    return None, None

def get_urls(num_pages, session):
    """
    Retrieves the list of property URLs to scrape.

    Args:
        num_pages (int): The number of pages to scrape.

    Returns:
        list: A list of property URLs.
    """
    list_all_urls = []
    global URL_COUNT  # Add the global variable for URL count
    for i in range(1, num_pages + 1):
        root_url = f"https://www.immoweb.be/en/search/house-and-apartment/for-sale?countries=BE&page={i}&orderBy=relevance"
        req = session.get(root_url)
        content = req.content
        soup = BeautifulSoup(content, "html.parser")
        if req.status_code == 200:
            # Find all property URLs on the page and add them to the list
            list_all_urls.extend(tag.get("href") for tag in soup.find_all("a", attrs={"class": "card__title-link"}))
            print(f'Urls found: {len(list_all_urls)}', end='\r', flush=True)
            URL_COUNT = len(list_all_urls)
        else:
            print("Page not found")
            break
    print(f"Number of properties: {len(list_all_urls)}")
    return list_all_urls



def save_raw_data(version):
    """
    Saves the raw scraped data to a CSV file.

    Args:
        version (int): The version number for the CSV file.
    """
    filename = f"data/scrapes/scrape_v{version}.csv"
    raw_data_df = pd.DataFrame(raw_data)
    os.makedirs("data/scrapes", exist_ok=True)
    raw_data_df.to_csv(filename, index=False)
    print(f"Raw data saved to {filename}")

def save_data(version):
    """
    Saves the scraped data to a CSV file.

    Args:
        version (int): The version number for the CSV file.
    """
    filename = f"data/filtered_data/house_details_v{version}.csv"
    house_details_df = pd.DataFrame(house_details)
    if not house_details_df.empty:
        house_details_df.replace({np.nan: 0, None: 0}, inplace=True)
        os.makedirs("data/filtered_data", exist_ok=True)
        house_details_df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")

def get_latest_version(file_prefix):
    """
    Retrieves the latest version number for the given file prefix.

    Args:
        file_prefix (str): The prefix of the CSV file.

    Returns:
        int: The latest version number.
    """
    os.makedirs("data/filtered_data", exist_ok=True)
    version = 0  # Initialize version as 0
    for filename in os.listdir("data/filtered_data"):
        if filename.startswith(file_prefix) and filename.endswith(".csv"):
            match = re.search(r"v(\d+)", filename)
            if match:
                file_version = int(match.group(1))
                version = max(version, file_version)  # Update version if higher version found
    return version

def load_data():
    """
    Loads previously scraped data from a CSV file.

    Returns:
        int: The version number of the CSV file.
    """
    latest_version = get_latest_version("house_details_v")
    version = latest_version + 1 if latest_version > 0 else 1
    filename = f"data/filtered_data/house_details_v{version}.csv"
    if latest_version > 0:
        print(f"Existing data found: version {latest_version}.")
    else:
        print(f"No existing data found.")
        print(f"Creating a new version {version} CSV file.")
        os.makedirs("data/filtered_data", exist_ok=True)
        with open(filename, 'w') as file:
            file.write('')  # Create an empty file

    return version


def process_url(url, session):
    """
    Processes a property URL and extracts the relevant details.

    Args:
        url (str): The URL of the property.
    """
    global ERROR_COUNT, URL_COUNT
    if any(record.get("id") == url for record in house_details):
        # Skip if URL already processed
        print(f"Skipping URL: {url}")
        return
    house_dict, raw_json_data = get_property(url, session)
    global SCRAPED_URLS
    if url in SCRAPED_URLS:
        # Skip if URL already processed
        print(f"Skipping URL: {url}")
        return
    if house_dict:
        filtered_house_dict = {}
        for new_key, old_key in selected_values:
            nested_keys = old_key.split(".")
            value = house_dict
            for nested_key in nested_keys:
                if isinstance(value, dict) and nested_key in value:
                    value = value[nested_key]
                else:
                    value = None
                    break
            filtered_house_dict[new_key] = value
        id_match = re.search(r"/(\d+)$", url)
        if id_match:
            filtered_house_dict["id"] = int(id_match.group(1))
        filtered_house_dict["Property url"] = url  # Add "Property url" field
        house_details.append(filtered_house_dict)
        raw_data.append({"url": url, "json_data": raw_json_data})
    else:
        # Increment error count
        ERROR_COUNT += 1
        # Sleep for 3 seconds if property details couldn't be fetched
        time.sleep(3)
def process_url_wrapper(url):
    """
    Wrapper function for processing a property URL.

    Args:
        url (str): The URL of the property.
    """
    global COUNTER, URL_COUNT, ERROR_COUNT, SCRAPED_URLS, COUNTER_LOCK
    with COUNTER_LOCK:
        COUNTER += 1
        print(f"URLs processed: {COUNTER}", end='\r', flush=True)
        # Use end='\r' and flush=True to stay on the same line

    process_url(url, session)
def run_scraper(num_pages, num_workers):
    # Check if previous scraped data exists
    version = load_data()  # Load previously scraped data
    list_of_urls = get_urls(num_pages, session)
    max_threads = min(num_workers, len(list_of_urls))
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        for _ in executor.map(process_url_wrapper, list_of_urls):
            pass
    # Calculate and print the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script finished in {elapsed_time:.2f} seconds.")
    # Save the final data and raw data
    print(f"\nTotal URLs processed: {COUNTER}, Total URLs found: {URL_COUNT}, Total errors: {ERROR_COUNT}\n")
    save_data(version)
    #save_raw_data(version)
    print(f"\nTotal records: {len(house_details)}\n")