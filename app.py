# Imports
import os

# Imports from other .py scripts
from helpers_sqldb import insert_data_to_db
from helpers_scrape import scrape_for_new_jobs
from helpers_translation_ai import translate_job_listings
from helpers_other import get_jobs_without_description_scrape_and_translate

# URL to scrape
url = os.environ['SCRAPE_URL']

#  -----------------  Main Code  -----------------
job_listings_to_translate = scrape_for_new_jobs(url)

# # Translate the Job titles from Greek to English and recreate the JSON file
translated_listings = translate_job_listings(job_listings_to_translate)
    
# Import the JSON into PGSQL database
insert_data_to_db(translated_listings)

# Scrape the description of new jobs, translate if in Greek and update the DB
get_jobs_without_description_scrape_and_translate()

