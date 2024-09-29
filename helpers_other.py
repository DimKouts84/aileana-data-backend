import json, time, random, datetime, os, time
from helpers_sqldb import connect_pg_conn
from helpers_translation_ai import translate_job_description
from helpers_scrape import scrape_job_description
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv(override=True)

########################     Variables    ########################
# NEO4J GRAPH DB Credentials
neo4j_url = os.getenv("NEO4J_CONNECTION_URL")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")
# Connect to the neo4j database
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

random_number = random.uniform(5, 8)

# PostgreSQL DB Credentials
host = os.getenv("POSTGRES_HOST")
database = os.getenv("POSTGRES_DB")
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASS")


# Get the list of all listing urls from the database in a list where the job description is empty
# Then scrape the job description for each listing url and update the database with the job description
def get_jobs_without_description_scrape_and_translate():
    cur, conn = connect_pg_conn(host, database, username, password)
    cur.execute("""
        SELECT listing_url FROM job_listings WHERE job_description IS NULL OR job_description = ''
    """)
    list_of_listing_urls = [row[0] for row in cur.fetchall()]
    conn.close()

    for listing_url in list_of_listing_urls:
        # print(listing_url)
        print(f" <-------------------------------------------->")
        print(f"Parsing job with URL ---> {listing_url}\n \n")
        print(f" <-------------------------------------------->\n \n")
        job_description = scrape_job_description(listing_url)
        # print(job_description)
        #  Translate the job description if in Greek in a try statement.
        # Try method for job listings that are not accessible anymore or the there was and API error (e.g. a limits issue)
        try:
            translated_job_description = translate_job_description(job_description)
        except Exception as e:
            print(f"Translation API call failed: {e}")
            for _ in range(5):
                time.sleep(30)
                try:
                    translated_job_description = translate_job_description(job_description)
                    break
                except Exception as e:
                    print(f"Translation API call failed: {e}")
            else:
                print(f"Translation API call failed after maximum retries.\n \n---------------------\nFor Job description:\n{job_description}")
                translated_job_description = None
        # print(translated_job_description)
        
        # Update job_listings table with Job Description
        cur, conn = connect_pg_conn(host, database, username, password)
        cur.execute("""
            UPDATE job_listings
            SET job_description = %s
            WHERE listing_url = %s
        """, (translated_job_description, listing_url))

        # Commit changes
        conn.commit()
        # Close cursor and connection
        cur.close()
        conn.close()
        
        # print(f"\rJob description has been updated. URL ---> {listing_url}.\n \n")
        # Wait a while before scraping the next job description
        time.sleep(random_number-2.9)
    print("All job descriptions have been updated.")
    return

### Nuke Neo4j database
def nuke_neo4j_db(session):
    # Delete all nodes and relationships
    session.run("MATCH (n) DETACH DELETE n")
    
    # Drop all indexes
    session.run("""
        CALL db.indexes() YIELD name
        CALL apoc.cypher.run('DROP INDEX ' + name, {}) YIELD value
        RETURN value
    """)
    
    # Drop all constraints
    session.run("""
        CALL db.constraints() YIELD name
        CALL apoc.cypher.run('DROP CONSTRAINT ' + name, {}) YIELD value
        RETURN value
    """)
    
    # Drop remaining schema using APOC
    session.run("CALL apoc.schema.assert({}, {})")
    print("Neo4j database has been nuked.")
    return


# A function to clear the job descriptions that start with a specific text, that usually comes from a LLM output. E.g. "Here is the translation:"
no_relevant_text = "Here is the translation:"
def get_jobdescriptions_with_no_relevant_text_and_clean():    
    # Query all job descriptions from the database
    cur, conn = connect_pg_conn(host, database, username, password)
    cur.execute("""
        SELECT job_description, reference FROM job_listings
    """)
    job_descriptions = [row for row in cur.fetchall()][:1]
    print(job_descriptions)

    for job_description in job_descriptions[0]:
        # Check if the description starts with "no_relevant_text"
        if job_description.startswith(no_relevant_text):
            # Remove the line "no_relevant_text"
            new_description = job_description.replace(no_relevant_text, "")
            print(f" New cleaned job description -->\n{new_description}\n Reference: {job_descriptions[1]}")
            # Update the description in the database
            # update_job_description_data(new_description, job_descriptions[1])
    # Commit the changes
    conn.commit()
    # Close the connection
    cur.close()
    conn.close()

# # Execute the clear_database function within a session
# with driver.session() as session:
#     clear_database(session)
# driver.close()
