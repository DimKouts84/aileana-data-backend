import json, time, random, datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from helpers_sqldb import get_list_with_ref_id, update_job_description_data
from helpers_translation_ai import translate_job_description, translate_job_listings

# A random number to have as time before actions, between x and y seconds.
random_number = random.uniform(3, 6)

# --------------------------------------------------------------------------
# This function get the URL of a website and scrapes for the available jobs, the title and the URL for each job
def scrape_for_new_jobs(url):
    # How many times robot will click the button for more jobs
    num_clicks = 100

    # Set the condition to stop scraping
    stop_condition = False

    # Initialize the data storage
    job_listings = []
    
    # Set up the browser
    # options = Options()
    # options.add_argument("--headless")
    # driver = webdriver.Chrome(options=options)
    driver = webdriver.Chrome()

    # Navigate to the page
    driver.get(url)  # Replace with the URL of the page

    # Get the HTML content
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # Wait for the cookie popup to appear and click on it
    try:
        cookie_popup = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".css-10d0ll5"))
        )
        # Click the "ΣΥΜΦΩΝΩ" button to accept the cookie policy
        cookie_popup.click()
    except:
        pass

    # Wait for the page to load
    time.sleep(random_number)

    # Get more job listing by clicking for more....
    for i in range(num_clicks):
        # Make a try condition, in case something goes wrong, then the process will finish with the data collected up until that time
        try:
            # Scroll to the bottom of the page
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Find the element and click
            while True:
                try:
                    element = WebDriverWait(driver, random_number).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Περισσότερες Αγγελίες...')]"))
                    )
                    driver.execute_script("arguments[0].click();", element)
                    break
                except StaleElementReferenceException:
                    pass
                
            # Scroll to the bottom of the page after geting more job listings.
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Update the HTML content
            html = driver.page_source
                
            # Wait for a few seconds and print the progress.
            time.sleep(random_number)
            print(f"\rPage: {i} --> Progress: [{'#' * int((i / num_clicks) * 20)}{' ' * (20 - int((i / num_clicks) * 20))}] --> {int(i / num_clicks * 100)}%", end='')
            # Check the stop condition
            if stop_condition:
                break
            
        except Exception as e:
            print(f"Error occurred at page {i}: {str(e)}")
            stop_condition = True
            break

    # Parse the HTML and get all Job listings of the front page
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('article', class_='search-result-card')

    # Loop all elements to get the data.
    for article in articles:
        job_listing_title = article.h2.a.text.strip()
        job_listing_details_Reference = article.find('a', class_='text-orane ref-number font-weight-bold').text.strip()
        job_listing_details_company_name = article.find("div", class_="card-contact-block").find_all("p")[1].text.strip()
        listing_url = article.h2.a['href']
        
        # Add the data into a JSON object
        job_listing_data = {
            "Job Listing Title": job_listing_title,
            "Job Listing Details Reference": job_listing_details_Reference,
            "Job Listing Company Name": job_listing_details_company_name,
            "Listing URL": listing_url
        }
        
        # Append to a list if the job is NOT ALREADY in the database using the Reference ID of the job.
        existing_jobs = get_list_with_ref_id()
        if job_listing_details_Reference not in existing_jobs:
            job_listings.append(job_listing_data)
        else:
            # print(f"Job already in the database ----> {job_listing_data}")
            pass
        # print(job_listing_data,"\n")

    # Close the browser
    driver.quit()

    # Write the list of job listings to a JSON file
    with open(f'{datetime.date.today()}_job_listings.json', 'w', encoding='utf-8') as f:
        json.dump(job_listings, f, ensure_ascii=False)
    
    return job_listings
 

# --------------------------------------------------------------------------
# This function gets a URL for a job, scrapes the job description and outputs as text.
def scrape_job_description(job_url):
    # Setup browser and options for Selenium

    # # Wait for the cookie popup to appear and click on it
    # try:
    #     cookie_popup = WebDriverWait(driver, 3).until(
    #         EC.element_to_be_clickable((By.CSS_SELECTOR, ".css-10d0ll5"))
    #     )
    #     # Click the "ΣΥΜΦΩΝΩ" button to accept the cookie policy
    #     cookie_popup.click()
    # except:
    #     pass

    # # Wait for the page to fully load
    # WebDriverWait(driver, 1).until(
    #     EC.presence_of_element_located((By.CSS_SELECTOR, 'div.description-part.alpha'))
    # )

    try:
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)

        # Navigate to the page
        driver.get(f'https://www.ergodotisi.com/{job_url}')

        
        # Get the HTML source
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # Find the HTML section
        description_part = soup.find('div', class_='col-md-12 description-part alpha')

        # Extract the text
        text = ''
        if description_part:
            for element in description_part.find_all(['p', 'li', 'ul']):
                if element.text.strip():
                    text += element.text.strip() + '\n'
            # Close the browser
        driver.quit()
        
    except:
        text = 'No description'
        print(f" <-------------------------------------------->")
        print(f"Error occurred while scraping the job description: {job_url}\n \n")
        pass
    
    # Wait a while before scraping the next job description
    time.sleep(random_number-2.9)

    # print(text)
    return text


# --------------------------------------------------------------------------
# This function gets the job listing, uses the listing URl and then A) Scraped the description, B) Translates it if in Greek and C) updates the database wit hthe translated job description.
def get_job_description(job_listings):
    for job_listing in job_listings:
        # Use the URL to scrape the job description
        job_url = job_listing["Job Listing URL"]
        # Scrape the job description
        job_description = scrape_job_description(job_url)
        #  Translate the job description if in Greek
        translated_job_description = translate_job_description(job_description)
        # Use the reference ID to update the job description in the database
        reference = job_listing["Job Listing Reference"]
        # Update the job description in the database
        update_job_description_data(translated_job_description, reference)
        # print(f"Job Description for {reference} has been updated.")
        # Wait a while before scraping the next job description
        time.sleep(random_number-3.1)
    return

# ---------------------------# Tests # ---------------------------#
# Tests the sraping for new jobs fro mthe website.
# url = os.environ['SCRAPE_URL']
# scrape_for_new_jobs(url)

# Tests the function for retrieving the bpdy of a job listing.
# job_url = 'JobDetails/248740/ZEMCO-GROUP'
# scrape_job_description(job_url)
