import asyncio
import aiohttp
import json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time
import os
import re
# Global variables
house_details = []
SCRAPED_URLS = set()
FETCH_ERROR_COUNT = 0
PROCESS_ERROR_COUNT = 0
URL_COUNT = 0
COUNTER = 0
COUNTER_LOCK = asyncio.Lock()
START_TIME = None
ERROR_REPORT = []  # List to store error information
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
async def get_property(session, url):
    """
    Fetches the property details from the given URL.
    Args:
        session (aiohttp.ClientSession): The session object to use for making the GET request.
        url (str): The URL of the property.
    Returns:
        dict: The property details as a dictionary.
    """
    global FETCH_ERROR_COUNT
    max_retries = 3  # Maximum number of retries
    retry_delay = 25  # Delay in seconds between retry attempts
    t_error = 0  # Function internal counter
    for retry_count in range(max_retries):
        try:
            async with session.get(url, timeout=60) as response:
                if response.status == 200:
                    try:
                        house_dict = await response.json(content_type=None)
                        house_dict["url"] = url  # Add the URL to the dictionary
                        return house_dict
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        # Increment fetch error count
                        async with COUNTER_LOCK:
                            FETCH_ERROR_COUNT += 1
                        return None
                else:
                    print(f"Failed to fetch URL: {url} (Status: {response.status})")
                    FETCH_ERROR_COUNT += 1
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if isinstance(e, asyncio.TimeoutError):
                t_error += 1
                print(f"Timeout error {t_error} occurred during scraping: {e}", end='\r')
            else:
                print(f"Error occurred during scraping: {e}")
            FETCH_ERROR_COUNT += 1
        await asyncio.sleep(retry_delay)  # Add a small delay between retry attempts

    print(f"Skipping URL: {url}")
    FETCH_ERROR_COUNT += 1
    return None
async def get_urls(num_pages, session):
    """
    Retrieves the list of property URLs to scrape.

    Args:
        num_pages (int): The number of pages to scrape.
        session (aiohttp.ClientSession): The session object to use for making the GET request.
    Returns:
        list: A list of property URLs.
    """
    list_all_urls = []
    global URL_COUNT
    for i in range(1, num_pages + 1):
        root_url = f"https://www.immoweb.be/en/search/house-and-apartment/for-sale?countries=BE&page={i}&orderBy=relevance"
        try:
            async with session.get(root_url) as response:
                content = await response.text()
            soup = BeautifulSoup(content, "html.parser")
            if response.status == 200:
                list_all_urls.extend(tag.get("href") for tag in soup.find_all("a", attrs={"class": "card__title-link"}))
                print(f'URLs found: {len(list_all_urls)}', end='\r', flush=True)
                URL_COUNT = len(list_all_urls)
            else:
                print("Page not found.")
                break
        except aiohttp.ClientError as e:
            print(f"Error occurred while retrieving URLs: {e}")
            break
    print(f"Number of properties: {len(list_all_urls)}")
    return list_all_urls
async def save_data(version):
    """
    Saves the scraped data to a CSV file.
    Args:
        version (int): The version number for the CSV file.
    """
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filename = os.path.join(root_folder, f"data/filtered_data/house_details_v{version}.csv")
    house_details_df = pd.DataFrame(house_details)
    if not house_details_df.empty:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        house_details_df.replace({None: np.nan}, inplace=True)
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
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    version = 0
    for filename in os.listdir(os.path.join(root_folder, "data/filtered_data")):
        if filename.startswith(file_prefix) and filename.endswith(".csv"):
            match = re.search(r"v(\d+)", filename)
            if match:
                file_version = int(match.group(1))
                version = max(version, file_version)
    return version
async def load_data():
    """
    Loads previously scraped data from a CSV file.

    Returns:
        int: The version number of the CSV file.
    """
    latest_version = get_latest_version("house_details_v")
    version = latest_version + 1 if latest_version > 0 else 1
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    filename = os.path.join(root_folder, f"data/filtered_data/house_details_v{version}.csv")
    if latest_version > 0:
        print(f"Existing data found: version {latest_version}.\nNew version {version} will be created.")
    else:
        print(f"No existing data found.")
        print(f"Creating a new version {version} CSV file.")
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as file:
            file.write('')
    return version
async def process_url(url, session):
    """
    Processes a property URL and extracts the relevant details.
    Args:
        url (str): The URL of the property.
        session (aiohttp.ClientSession): The session object to use for making the GET request.
    """
    global SCRAPED_URLS, COUNTER_LOCK, FETCH_ERROR_COUNT, PROCESS_ERROR_COUNT, ERROR_REPORT, COUNTER
    async with COUNTER_LOCK:
        if url in SCRAPED_URLS:
            return
        SCRAPED_URLS.add(url)
    house_dict = await get_property(session, url)
    async with COUNTER_LOCK:
        if url in SCRAPED_URLS:
            # Skip if URL already processed
            return
    if house_dict is None:
        # Increment fetch error count
        async with COUNTER_LOCK:
            FETCH_ERROR_COUNT += 1
        # Sleep for 3 seconds if property details couldn't be fetched
        await asyncio.sleep(3)
        return
    try:
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
        async with COUNTER_LOCK:
            house_details.append(filtered_house_dict)
    except Exception as e:
        print(f"Error occurred during processing: {e}")
        # Increment process error count
        async with COUNTER_LOCK:
            PROCESS_ERROR_COUNT += 1
        # Add error information to the report
        ERROR_REPORT.append(f"Error occurred during processing URL: {url}\n{str(e)}")
    async with COUNTER_LOCK:
        COUNTER += 1
async def process_url_wrapper(url, session):
    """
    Wrapper function for processing a property URL.
    Args:
        url (str): The URL of the property.
        session (aiohttp.ClientSession): The session object to use for making the GET request.
    """
    if url in SCRAPED_URLS:
        return
    await process_url(url, session)
    await asyncio.sleep(1)  # Add a delay of 1 second between requests
    async with COUNTER_LOCK:
        global COUNTER
        COUNTER += 1
async def print_progress():
    """
    Prints the progress of URL processing.
    """
    global COUNTER, URL_COUNT, FETCH_ERROR_COUNT, PROCESS_ERROR_COUNT, START_TIME
    while COUNTER < URL_COUNT:
        elapsed_time = time.time() - START_TIME
        print(f"URLs processed: {COUNTER}/{URL_COUNT}, Fetch errors: {FETCH_ERROR_COUNT}, Process errors: {PROCESS_ERROR_COUNT}, Elapsed time: {elapsed_time:.2f} seconds", end='\r', flush=True)
        await asyncio.sleep(0.1)
    print(f"URLs processed: {COUNTER}/{URL_COUNT}, Fetch errors: {FETCH_ERROR_COUNT}, Process errors: {PROCESS_ERROR_COUNT}, Elapsed time: {elapsed_time:.2f} seconds")
async def run_scraper(num_pages):
    """
    Runs the scraper to fetch property details.
    Args:
        num_pages (int): The number of pages to scrape.
    Returns:
        int: The version number of the CSV file.
    """
    global START_TIME, ERROR_REPORT
    try:
        START_TIME = time.time()
        version = load_data()
        connector = aiohttp.TCPConnector(limit=5)  # Define the connector
        async with aiohttp.ClientSession(connector=connector) as session:
            urls = await get_urls(num_pages, session)
            global URL_COUNT
            URL_COUNT = len(urls)
            tasks = []
            for url in urls:
                task = process_url_wrapper(url, session)
                tasks.append(task)
            progress_task = asyncio.create_task(print_progress())
            await asyncio.gather(*tasks)
            progress_task.cancel()
        await save_data(version)
        total_time = time.time() - START_TIME
        print("Scraping completed!")
        print(f"Final Summary: URLs scraped: {URL_COUNT}, URLs processed: {COUNTER}, Fetch errors: {FETCH_ERROR_COUNT}, Process errors: {PROCESS_ERROR_COUNT}, Total time: {total_time:.2f} seconds")
        # Print error report
        if ERROR_REPORT:
            print("\n--- Error Report ---")
            for error in ERROR_REPORT:
                print(error)
        return version
    except Exception as e:
        print(f"An error occurred: {e}")
        raise