from helper_llm_functions import call_groq_JSON, prompts, extract_system_prompt, llm_to_be_used
from helper_llm_graph_db import import_job_data_to_neo4j, create_relationship_skill_and_responsibilities_in_neo4j
# from helpers_sqldb import update_job_as_imported, get_jobs_not_imported_to_neo4j
import os, json, time
from dotenv import load_dotenv
from neo4j import GraphDatabase
from groq import Groq

load_dotenv(override=True)

########################   VARIABLES   ########################
# Credentials for GROQ API
Groqllm = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# NEO4J GRAPH DB Credentials
neo4j_url = os.getenv("NEO4J_CONNECTION_URL")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Connect to the neo4j database
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))




########################   FUNTIONS   ########################
### A function to extract data from the job listing for for each category/node ###
def extract_data(model, system_prompt, user_prompt):
    extracted_data = call_groq_JSON(model, system_prompt, user_prompt)
    return extracted_data

### Loop for all categories/nodes of data in a job listing text. The extracted data are combined in a JSON file. ###
def extract_data_in_batches(llm_to_be_used, system_prompt):
    final_json = {}
    for prompt in prompts:
        extracted_data = extract_data(llm_to_be_used, system_prompt, prompt)
        extracted_data = json.loads(extracted_data)  # Convert JSON string to dictionary
        time.sleep(12)  # Sleep for 8 seconds to avoid the API rate limit.
        # Merge extracted data with final_json
        final_json = {**final_json, **extracted_data}
        
        # ~ Save the JSON file for debugging ~ #
        with open(f'final_job_data.json', 'w', encoding='utf-8') as f:
            json.dump(final_json, f, ensure_ascii=False, indent=4)
    return final_json


#### ~~~ Testing the function ~~~ ####
structured_extracted_data = extract_data_in_batches(llm_to_be_used, extract_system_prompt)
# print(f"Extracted data -> \n\n{structured_extracted_data}")

 
with driver.session() as session:
    from helpers_sqldb import get_jobs_not_imported_to_neo4j
    ############# Choose a job listing from the list of job listings not imported to Neo4j ~~~ FOR TESTING REASONS ~~~ #############
    job_data_not_imported_to_neo4j = get_jobs_not_imported_to_neo4j()[635]
    # print(type(job_data_not_imported_to_neo4j), "\n -----------> ", job_data_not_imported_to_neo4j)
    job_reference = job_data_not_imported_to_neo4j['job_reference']
    job_description = job_data_not_imported_to_neo4j['job_description']
    country = "Cyprus"
    import_job_data_to_neo4j(session, structured_extracted_data, job_reference, country, job_description)
    # print(f"~~~~~~~~ Job Listing ~~{job_reference}~~ imported to Neo4j.\n")
    create_relationship_skill_and_responsibilities_in_neo4j(session, structured_extracted_data)
    # print(f"~~~~~~~~ Relationships created in Neo4j for company ~~{job_reference}~~.\n")

# Close the driver
driver.close()

