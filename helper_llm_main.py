""" ALL THE IMPORTS """
# Imports of necessary libraries
import os, json, time, random, logging, datetime, psycopg2, requests, time
from dotenv import load_dotenv
from neo4j import GraphDatabase
from groq import Groq
from pydantic import BaseModel, ValidationError
from typing import List, Optional

# Imports from other .py scripts
from helpers_sqldb import get_jobs_not_imported_to_neo4j, import_job_data_to_neo4j

load_dotenv(override=True)

#  -----------------     Variables    ----------------- #
country = "Cyprus"
random_number = random.uniform(3, 6)

# PostgreSQL DB Credentials and Connection
host = os.getenv("POSTGRES_HOST")
database = os.getenv("POSTGRES_DB")
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASS")

# Initiate a connection to the pgDB and return cur & conn
def connect_pg_conn(host, database, username, password):
    # Connect to Postgres
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=username,
        password=password,
        # timeout=30  # Increase the connection timeout to 30 seconds
    )
    cur = conn.cursor()
    return cur, conn

# NEO4J GRAPH DB Credentials and Connection
neo4j_url = os.getenv("NEO4J_CONNECTION_URL")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Connect to the neo4j database
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

#### Available Cloud (GROQ) setup and models
Groqllm = Groq(api_key=os.environ.get("GROQ_API_KEY"))
llama8B, llama70B, mixtral_8x7b, llama_31_70b, llama_31_8b = "llama3-8b-8192", "llama3-70b-8192", "mixtral-8x7b-32768", "llama-3.1-70b-versatile", "llama-3.1-8b-instant"

#### Available Local (Ollama) request URLs and models
ollama_embed_url = os.getenv("OLLAMA_EMBEDD_URL")
bge_m3, nomic_embed_text = 'bge-m3', 'nomic-embed-text'

ollama_completions_url = os.getenv("OLLAMA_COMPLETIONS_URL")
ollama_chat_completions_url = os.getenv("OLLAMA_CHAT_COMPLETIONS_URL")
o_llama_31_8b_q4, o_llama_31_8b_fp16, o_llama_32_3B, o_phi_35_8B = "llama3.1:8b","llama3.1:8b-instruct-fp16","llama3.2:3b-instruct-fp16", "phi3.5:3.8b-mini-instruct-q8_0"

# Confifgure the logger and the timestamp
timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logging.basicConfig(filename='error_log.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#  -----------------   Pydantic Models for Data Validation    ----------------- #
class Industry(BaseModel):
    industry_name: str
    NACE_industry_name: str

class StandardizedOccupation(BaseModel):
    isco_code: str
    isco_name: str

class Skill(BaseModel):
    skills_category: str
    skills_name: str
    skills_type: str

class CertificationDegree(BaseModel):
    certification_name: str

class AcademicDegree(BaseModel):
    academic_degree_name: Optional[str] = None
    academic_degree_field: Optional[str] = None

class Experience(BaseModel):
    experience_required: bool
    years_of_experience: int

class Benefit(BaseModel):
    benefit_name: str

class Responsibility(BaseModel):
    responsibility_name: str

class Job(BaseModel):
    job_title: str
    standardized_occupation: StandardizedOccupation
    job_seniority: Optional[str]  = None
    minimum_level_of_education: int  = None
    employment_type: Optional[str] = None
    employment_model: Optional[str] = None

class JobListing(BaseModel):
    job_reference: str
    job_description: str
    industry: Industry
    job: Job
    skills: List[Skill]
    certification_degree: List[CertificationDegree]
    academic_degree: List[AcademicDegree]
    experience: Experience
    benefits: List[Benefit]
    responsibilities: List[Responsibility]

def validate_job_listing(data: dict) -> bool:
    try:
        job_listing = JobListing(**data)
        # print("Validation successful!")
        return True
    except ValidationError as e:
        print("Validation failed!")
        print(e.json())
        return False


# Function to open the system file. Files likes user or system prompts and instructions.
def open_prompt_files(file):
    with open(file, 'r', encoding='utf-8') as f:
        promt_data = f.read()
    return promt_data

""" ----------------- Helper Functions ----------------- """
###  -----------------  API LLM Requests ----------------- ###
# Function to call Groq API LLms in JSON mode
def call_groq_JSON(model, system_prompt, user_prompt_for_parsing):
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    chat_completion = Groqllm.chat.completions.create(
        messages=[{"role": "system","content": f"{system_prompt}"},{"role": "user","content": f"{user_prompt_for_parsing}, {llm_instructions}"}],
        model=model,
        temperature=0,
        stream=False, # Streaming is not supported in JSON mode
        response_format={"type": "json_object"}, # Enable JSON mode by setting the response format
        # max_tokens=8192
        )
    try:
        answer_text = str(chat_completion.choices[0].message.content)
        # print(answer_text)
    except Exception as e:
        print(e)
        answer_text = f"Error: {e} response from AI model."
    # print(answer_text) #for debugging
    return answer_text

# Function to call Local LLM using the Ollama API in JSON mode
def call_ollama_JSON(model, system_prompt, user_prompt_for_parsing):
    url = ollama_chat_completions_url
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": f"{system_prompt}"
            },
            {
                "role": "user", 
                "content": f"{user_prompt_for_parsing}, {llm_instructions}"
            }
        ],
        "temperature": 0,  
        "format": "json",
        "stream": False
    }
    headers = {
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        # print(f"Response text: \n{response.text}\n\n")
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()['message']['content']
    except requests.RequestException as e:
        print(f"Error: {e} response from Ollama API.")
        return f"Error: {e} response from Ollama API."

# A function to validate the job listing data
def validate_job_listing(data: dict) -> bool:
    try:
        job_listing = JobListing(**data)
        print("Validation successful!")
        return True
    except ValidationError as e:
        print("\n --------- Validation failed !!!  ---------\n")
        logging.error(f"{timestamp} | Validation failed for job: {job_listing}")
        return False

###  -----------------  System & User Prompts for Data Extraction and Structuring ----------------- ###
### A function to extract data from the job listing for for each category/node ###
def extract_data(model, system_prompt, user_prompt):
    # extracted_data = call_groq_JSON(model, system_prompt, user_prompt)
    extracted_data = call_ollama_JSON(model, system_prompt, user_prompt)
    return extracted_data

### Loop for all categories/nodes of data in a job listing text. The extracted data are combined in a JSON file. ###
# The function outputs the extracted data in a JSON format after succesful validation
def extract_data_in_batches(llm_to_be_used, system_prompt, job_listing_not_imported, max_retries=5):
    for attempt in range(max_retries):
        final_json = {}
        for prompt in prompts:
            prompt = prompt + job_listing_not_imported['job_description']
            extracted_data = extract_data(llm_to_be_used, system_prompt, prompt)
            extracted_data = json.loads(extracted_data)
            general_job_information = {"job_reference": job_listing_not_imported['job_reference'], "job_description": job_listing_not_imported['job_description']}
            
            time.sleep(60)  # Wait to avoid the API rate limit.

            # Merge extracted data with final_json
            final_json = {**final_json, **general_job_information, **extracted_data}

            # ~~~ Debugging ~~~ #
            logging.info(f"{timestamp} | Processing with {llm_to_be_used}")
            print(f"Using model: '{llm_to_be_used}' now")

        # Save intermediate JSON for debugging or future use
        with open(f'final_job_data.json', 'w', encoding='utf-8') as f:
            json.dump(final_json, f, ensure_ascii=False, indent=4)

        # Validate the final JSON
        if validate_job_listing(final_json):
            return final_json  # Return the valid final_json
        
        print(f"Validation failed on attempt {attempt + 1}. Restarting extraction...")

    raise ValueError("Max retries reached. Job listing extraction failed validation.")


""" ----------------- Prompting Variables ----------------- """
########################    The PROMPTS for parsing the job listing text   ########################
parsed_industry_data = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_job_industry.json") + "\n"
    + f" *** The industry standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_NACE.txt") + "\n"
    + f" Focus on extracting the industry data of a job listing!" + "\n"
    + f" Do not forget to extract the data in a JSON format!" + "\n"
    + f" *** The job listing text is the following *** :\n")

parsed_main_data = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_main_job_data.json") + "\n"
    + f" *** The ISCO standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCO.txt") + f"Ensure that you use the correct ISCO code!" + "\n"
    + f" *** The ISCED standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCED.txt") + f"Ensure that you use the correct ISCED code!" + "\n"
    + f" Do not forget to extract the data in a JSON format!" + "\n"
    + f" *** The job listing text is the following *** :\n")

parsed_skills_and_qualifications = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following. *** :\n" + open_prompt_files(r"data\prompts\json_template_skills.json") + "\n"
    + f"Focus on extracting the skills and qualifications of a person!"
    + f" Do not forget to extract the data in a JSON format!" + "\n"
    + f" *** The job listing text is the following *** :\n")

parsed_experience_and_responsibilities = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_experience_benefits.json") + "\n"
    + f" Focus on extracting the benefits of a job listing and experience and responsibilities of a person!"
    + f" Do not forget to extract the data in a JSON format!" + "\n"
    + f" *** The job listing text is the following *** :\n")

llm_instructions = (f" *** INSTRUCTIONS: ***\n" "Think step by step how you would extract the data from the job listing text.\n"
    + " Follow the instructions in the user prompt, be ACCURATE and DO NOT HALLUCINATE.\n"
    + " Reflect upon your thoughts, make corrections.\n")

# Extraction of data in batches, to increase the accuracy of the extracted data:
prompts = [parsed_industry_data, parsed_main_data, parsed_skills_and_qualifications, parsed_experience_and_responsibilities]

""" ----------------- Job Data Processing and importing to Graph Database ----------------- """
def process_jobs_and_import_to_graphDB(driver, country):
    # Available open-source models for Groq LLM
    llama8B, llama70B, llama_31_8b, llama_31_70b = "llama3-8b-8192", "llama3-70b-8192", "llama-3.1-8b-instant", "llama-3.1-70b-versatile"
    
    # Get all the jobs that are not imported to the Graph DB
    all_job_not_into_graphDB = get_jobs_not_imported_to_neo4j()
    
    # The system prompt to be used for the LLM model
    extract_system_prompt = open_prompt_files("data/prompts/system_prompt_extract_data.txt")
    
    with driver.session() as session:
        # Loop through all the jobs that are not imported to the Graph DB and import them
        for job_data in all_job_not_into_graphDB:
            print(f"Job with {job_data['job_reference']}")
            retry_count, max_retries = 0, 5
            ## Groq Models
            # models = [o_llama_31_8b_q4, llama_31_70b, llama_31_8b]
            ## Ollama Models
            models = [o_llama_31_8b_q4, o_llama_31_8b_fp16, o_llama_32_3B, o_phi_35_8B]
            model_index = 0
            
            while model_index < len(models):
                current_model = models[model_index]
                print(f"Using model: '{current_model}' now")
                
                try:
                    job_data = extract_data_in_batches(current_model, extract_system_prompt, job_data)
                    # Break out of the model loop if successful
                    break 
                except Exception as e:
                    error_message = str(e)
                    print(f"{error_message}\nSwitching to next model due to error.")
                    logging.error(f"Error processing job {job_data['job_reference']} with {current_model} | {error_message}")
                    
                    # Move to the next model if there is an API error (like rate limiting)
                    if "rate limit" in error_message.lower() or "Error code: 429" in error_message:
                        model_index += 1
                        if model_index < len(models):
                            print(f"Switching to next model: '{models[model_index]}' due to rate limit error.")
                    else:
                        # Retry for other types of errors
                        retry_count += 1
                        if retry_count >= max_retries:
                            print(f"Max retries reached for job {job_data['job_reference']} with {current_model}.")
                            break
                        print(f"Retrying... {retry_count}/{max_retries}")
                        time.sleep(10)  # Wait to avoid the API rate limit
            
            # Import the job data to the Neo4j database if the process was successful
            if retry_count < max_retries:
                import_job_data_to_neo4j(session, job_data, job_data['job_reference'], country, job_data['job_description'])
            else:
                print(f"Failed to process job {job_data['job_reference']} with all models.")

            

# ----------------- Embedding data and populating databases ----------------- #
# Get all the parameters from a JOB node in the neo4j DB
def get_node_data_from_neo4J_job(driver, job_reference):
    with driver.session() as session:
        # Get the job node
        result = session.run("MATCH (j:JOB {job_reference: $job_reference}) RETURN j", job_reference=job_reference)
        job_node_params = result.single()["j"]
        
        # In case there are no job nodes with the given reference
        if job_node_params is None:
            print(f"Job node with reference {job_reference} not found.")
            return
        
        # print(f"Node properties:\n{str(job_node)}\n")
        return job_node_params


# Create a vector embedding from the text of a job listing data
def create_embedding_data(text, model):
    url = ollama_embed_url
    payload = {
        "input": text,
        "model": model
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    print(f"Embedding created!")
    return response.json()['embeddings'][0]

class LocalLLMError(Exception):
    pass

def create_embedding_data_with_retries(text, model, retries=5):
    for attempt in range(retries):
        try:
            return create_embedding_data(text, model)
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(20)  # Optional: wait a bit before retrying
    raise LocalLLMError("Local LLM is not working after multiple attempts.")

    
# Add the embedding to the job node in the neo4j DB
def add_embedding_to_NEO4J_job(embedding, job_reference):
    with driver.session() as session:
        # Store the embedding in the job node's 'embedding' property
        session.run("MATCH (j:JOB {job_reference: $job_reference}) CALL db.create.setNodeVectorProperty(j, 'embedding', $embedding)", job_reference=job_reference, embedding=embedding)
        print(f"Embedding added to Neo4J job node with reference {job_reference}.")


# Add the embedding to the job node in the PostgreSQL DB
def add_embedding_to_PG_job(embedding, job_reference):
    # Store the embedding into the PostgreSQL DB using the job_reference at a column named 'embedding'
    cur, conn = connect_pg_conn(host, database, username, password)
    with conn.cursor() as cur:
        cur.execute("UPDATE job_listings SET embedding = %s WHERE reference = %s", (embedding, job_reference))
        conn.commit()
        print(f"Embedding added to PostgreSQL column with reference {job_reference}.")

# """ ----------------- Testing ----------------- """
# # Test the extraction of data from a job listing - for debugging purposes
# def test_extraction_of_data_from_job_listing():
#     # The system prompt to be used for the LLM model
#     extract_system_prompt = open_prompt_files("data/prompts/system_prompt_extract_data.txt")
    
#     # Get all the jobs that are not imported to the Graph DB
#     all_job_not_into_graphDB = get_jobs_not_imported_to_neo4j()
    
#     # Extract data from a single job listing
#     job_data = all_job_not_into_graphDB[8759]
#     print(f"Job --> {job_data['job_title']}\n{job_data['job_description']}\n\n\n")
#     all_job_json = extract_data_in_batches(o_llama_31_8b_fp16, extract_system_prompt, job_data, max_retries=5)
#     # industry_json = extract_data(o_llama_31_8b_fp16, extract_system_prompt, parsed_industry_data+job_data['job_description'])
#     # save the extracted data to a JSON file
#     with open(f'extracted_job_data_for_test.json', 'w') as f:
#         json.dump(all_job_json, f)
#     return all_job_json

# test_extraction_of_data_from_job_listing()