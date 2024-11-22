""" ALL THE IMPORTS """
# Import necessary libraries
import os, json

# Imports from other .py scripts
from helpers_sqldb import insert_data_to_db, nuke_neo4j_db, reset_imported_status
from helpers_scrape import scrape_for_new_jobs
from helpers_translation_ai import translate_job_listings
from helpers_other import get_jobs_without_description_scrape_and_translate
from helper_llm_main import process_jobs_and_import_to_graphDB, job_rag_pipeline,driver

#  -----------------     Variables    ----------------- #
# URL to scrape
url = os.environ['SCRAPE_URL']

#  -----------------  Main Code  ----------------- #
# Scrape the URL for new jobs
job_listings_to_translate = scrape_for_new_jobs(url)

# # Translate the Job titles from Greek to English and recreate the JSON file
translated_listings = translate_job_listings(job_listings_to_translate)
    
# Import the JSON into PGSQL database
insert_data_to_db(translated_listings)

# Scrape the description of new jobs, translate if in Greek and update the PostgreSQL DB
get_jobs_without_description_scrape_and_translate()

# Uncomment the below code to reset the imported status of all the jobs in the DB and
# nuke_neo4j_db()
# reset_imported_status()

# Get all the jobs that are not imported to the Graph DB, process and import them to the Graph DB
process_jobs_and_import_to_graphDB(driver, country="Cyprus")

# Create embeddings for the job descriptions
job_rag_pipeline(driver)
