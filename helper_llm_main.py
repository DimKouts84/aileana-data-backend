""" ALL THE IMPORTS """
# Imports of necessary libraries
import os, json, time, random, logging, datetime, psycopg2, requests, time
from dotenv import load_dotenv
from neo4j import GraphDatabase
# from groq import Groq  ~~~~~~~ UNINSTALL THIS PACKAGE ~~~~~~~
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


#### Available Local (Ollama) request URLs and models
ollama_embed_url = os.getenv("OLLAMA_EMBEDD_URL")
bge_m3, nomic_embed_text = 'bge-m3', 'nomic-embed-text'

ollama_completions_url = os.getenv("OLLAMA_COMPLETIONS_URL")
ollama_chat_completions_url = os.getenv("OLLAMA_CHAT_COMPLETIONS_URL")
o_llama_31_8b_fp16, o_llama_32_3B_fp16, o_phi_35_8B, qwen25_7B = "llama3.1:8b-instruct-fp16","llama3.2:3b-instruct-fp16", "phi3.5:3.8b-mini-instruct-q8_0", "qwen2.5:7b-instruct-fp16"


lmstudio_chat_completions_url = os.getenv("LM_STUDIO_COMPLETIONS_URL")


# Confifgure the logger and the timestamp
timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logging.basicConfig(filename='error_log.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#  -----------------   Pydantic Models for Data Validation    ----------------- #

class industry(BaseModel):
    industry_name: str
    NACE_standardized_name: str

class occuation_details(BaseModel):
    job_seniority: str
    minimum_level_of_education: int
    employment_type: Optional[str]
    employment_model: Optional[str]

class skills(BaseModel):
    skills_category: str
    skills_name: str
    skills_type: str

class certifications(BaseModel):
    certification_name: str

class academic_degree(BaseModel):
    academic_degree_name: str
    academic_degree_field: str  
    
class experience(BaseModel):
    experience_required: bool
    years_of_experience: Optional[int]  # Allow None values
    
class benefits(BaseModel):
    benefit_name: str

class responsibilities(BaseModel):
    responsibility_name: str

class JobListing(BaseModel):
    job_reference: str
    job_description: str
    industry: industry
    job_title: str
    isco_name: str
    occuation_details: occuation_details
    skills: List[skills]
    certifications: List[certifications]
    academic_degree: List[academic_degree]
    experience: experience
    benefits: List[benefits]
    responsibilities: List[responsibilities]

    
# Function to open the system file. Files likes user or system prompts and instructions.
def open_prompt_files(file):
    with open(file, 'r', encoding='utf-8') as f:
        promt_data = f.read()
    return promt_data

def open_JSON_files(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

""" ----------------- Helper Functions ----------------- """
###  -----------------  API LLM Requests ----------------- ###

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
        "format": "json",
        "stream": False,
        # "temperature": 0
        "options": 
        {
            # "num_keep": 1, 
            # "top_p": 0.2,
            "temperature": 0.1
            # "mirostat": 1,
            # "mirostat_tau": 0.1,  
            # "mirostat_eta": 0.1,
            # "tfs_z": 0.1
        }
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



# Function to call Local LLM using the LMStudio (OPENAI) API in JSON mode
def call_lmstudio_JSON(model, system_prompt, user_prompt_for_parsing):
    # Prepare the headers
    url = lmstudio_chat_completions_url
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer "lm-studio',
    }

    # Prepare the messages for the chat completion
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt_for_parsing}
    ]
    # Prepare the payload
    payload = {
        'model': model,
        'messages': messages,
        "type": "json_object",
        'temperature': 0,
        'max_tokens': 9216
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        # print(f"Response text: \n{response.text}\n\n")
        response.raise_for_status()  # Raise an error for bad status codes
        # return response.json()['choices'][0]['message']['content']
        return response.json()['choices'][0]['message']['content']
    except requests.RequestException as e:
        print(f"Error: {e} response from LMStudio API.")
        return f"Error: {e} response from LMStudio API."


# A function to validate the job listing data
def validate_job_listing(data: dict) -> bool:
    try:
        data = JobListing(**data)
        print("Validation successful!")
        return True
    except ValidationError as e:
        print("\n --------- Validation failed !!!  ---------\n")
        logging.error(f"Validation error: {e.json()}")
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
    + f" *** The NACE Standards Clasicifation List is here: *** :\n" + open_prompt_files(r"data\prompts\standard_NACE.txt") + f"Ensure that you use text from the provided list!" + "\n"
    + f" *** Here are some examples of the extraction: *** :\n" + open_prompt_files(r"data\prompts\examples_job_industry.json") + "\n"
    + f" Do not forget to extract the data in the provided JSON schema!" + "\n"
    + f" *** The job listing text is the following *** :\n")

parsed_main_data = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_main_job_data.json") + "\n"
    + f" *** The ISCO Standard to choose from are here: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCO.txt") + f"Ensure that you use the correct ISCO code!" + "\n"
    + f" *** The ISCED Standard to choose from are here: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCED.txt") + f"Ensure that you use the correct ISCED code!" + "\n"
    + f" *** Here are some examples of the extraction: *** :\n" + open_prompt_files(r"data\prompts\examples_job_main_data.txt") + "\n"
    + f" Do not forget to extract the data in the provided JSON schema!" + "\n"
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
            models = [o_llama_32_3B_fp16, o_llama_31_8b_fp16, o_llama_32_3B_fp16, o_phi_35_8B]
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


# """ ---------------------------------- Testing ---------------------------------- """
# Get all the jobs that are not imported to the Graph DB
all_job_not_into_graphDB = get_jobs_not_imported_to_neo4j()
# Extract data from a single job listing
db_job_all_data = all_job_not_into_graphDB[12365]
db_job_data = [db_job_all_data['job_title'], db_job_all_data['job_description']]

# A function that takes the extracted data from two different LLM models and uses a third to define the correct output
def job_data_preprocessing_extraction_classification(model, db_job_data):
    print(f"Job description -->\n{db_job_data[0]}\n{db_job_data[1]}")
    print("\n\n")
    
    system_prompt = ("You have to follow the user's instructions and parse text data as requested."
    + "Follow instructions, DO NOT hallucinate and be accurate in your response."
    )

    #---------------------- Data preprocessing and extraction for Industry Data ----------------------#
    user_prompt_industry_summarization = ("You will read the description of a job posting."
    + "Then you will make a small description of company, what the company does and the industry this company operates."
    + "Here is the job description: "+ str(db_job_data)
    + "Your output must follow the JSON template: {'industry_summarization':'The description of the company and the industry this company operates in'}"
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_industry = call_lmstudio_JSON(model, system_prompt, user_prompt_industry_summarization)
    
    NACE_stabdardized_industry_title = [open_prompt_files(r'data\prompts\standard_NACE.txt')]
    user_prompt_industry_data_classification = (f" Read the information provided for a company and the industry it operates in. "
    + "You will have to provide the the NACE industry title, from the list provided below."
    + "The NACE Standards Clasicifation List is here: "+ str(NACE_stabdardized_industry_title  )  
    + f" *** The company information is the following *** {summarization_industry}:\n"
    + 'Your outpout must follow the JSON template:'
    + '{"industry": {"industry_name":"A title of the industry","NACE_standardized_name":"The NACE title from the list that matches the company industry"}}'
    + "Ensure to output EXACTLY the JSON format without any additional explanations!")
    
    # Check if the NACE classification is correct and loop until the correct classification is made
    while True:
        output_industry_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_data_classification))
        NACE_stabdardized_industry_title = [title.strip('"') for title in open_prompt_files(r'data\prompts\standard_NACE.txt').strip('[]').split(',\n')]

        if output_industry_classification['industry']['NACE_standardized_name'] in NACE_stabdardized_industry_title:
            print("The NACE classification is: ", output_industry_classification, "\n\n")
            break
        else:
            print(f"The NACE classification {output_industry_classification['industry']['NACE_standardized_name']} is incorrect. Please classify the NACE industry again.")
            output_industry_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_data_classification))
    
    
    #---------------------- Data preprocessing and extraction for Main Job Data ----------------------#
    user_prompt_for_summarization = ("You will read the description of a job posting. "
    +"Then you will summarize the description of the job in a sentence (including a description of the company, title and a other information)."
    +"Here is the job description: "+ str(db_job_data)
    +'Your output must follow the JSON template: {"job_title_description":"The job description in a sentence"}'
    +"Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_job_title = call_lmstudio_JSON(model, system_prompt, user_prompt_for_summarization)
    
    user_prompt_for_job_title = ("You will receive information about the job title of a job posting."
    + "You will output ONLY the title of the job. Do not include the company name or any other information, just the job title"
    + "The job information is the following: "+ str(summarization_of_job_title)
    + 'Your output must follow the JSON template: {"job_title":"The job title"}'
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    output_job_title = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_job_title))
    print("The job title is: ", output_job_title, "\n\n")
    
    ISCO_stabdardized_occupation_title = [open_prompt_files(r'data\prompts\standard_ISCO.txt')]
    user_prompt_for_ISCO_classification = ("You will receive job information about the job title of a job posting."
    +"You will have to classify the ISCO title based on the job description."
    +"The job title is the following: "+ summarization_of_job_title +"\n"
    +"Here is a list of ISCO titles is:"+ str(ISCO_stabdardized_occupation_title)
    +"Step A: Read all ISCO titles"
    +"Step B: Think step by step and choose the ISCO title that matches the job description!\n" 
    +'Your output must follow the JSON template: {"isco_name":"The ISCO title from the provided list"}'
    +"Ensure that you choose the correct ISCO title in JSON format without any additional explanations.")
    
    # Check if the ISCO classification is correct and loop until the correct classification is made
    while True:
        output_ISCO_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_ISCO_classification))
        ISCO_stabdardized_occupation_title = [title.strip('"') for title in open_prompt_files(r'data\prompts\standard_ISCO.txt').strip('[]').split(',\n')]

        if output_ISCO_classification['isco_name'] in ISCO_stabdardized_occupation_title:
            print("The ISCO classification is: ", output_ISCO_classification, "\n\n")
            break
        else:
            print(f"The ISCO classification {output_ISCO_classification['isco_name']} is incorrect. Please classify the ISCO title again.")
            output_ISCO_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_ISCO_classification))

    #---------------------- Data preprocessing and extraction for Experience and Employment Data ----------------------#
    user_prompt_for_experience_and_employment = ("You will read the description of a job posting."
    +"Then you will summarize, in one sentence: a) how many years experience is required for this job, b) what type of employment is offered (full time, part-time, remote, hybrid etc) c) the education level and degree required for this job."
    +"Here is the job description: "+ str(db_job_data)
    +'Your output must follow the JSON template: {"experience_and_employment":"Summarization of the minimum years of experience required, educational level or degrees required and the employment type"}'
    +"Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_experience_and_employment = call_lmstudio_JSON(model, system_prompt, user_prompt_for_experience_and_employment)
    
    ISCED_stabdardized_occupation_title = [open_prompt_files(r'data\prompts\standard_ISCED.txt')]
    user_prompt_for_experience_and_employment_classification = ("You will receive information about the experience required and employment type of a job posting."
    + "You will A) Read the information provided B) classify the job seniority C) classify the minimum level of education required based on ISCED"
    + "D) employment Type and E) employment model for this job.\n"
    + "The job information is the following: "+ str(summarization_of_experience_and_employment)
    + " *** The ISCED Standard to choose from are here: *** :" + str(ISCED_stabdardized_occupation_title)
    + "Your output must follow the JSON template:\n"
    + '{"occuation_details":{"job_seniority": " "Internship", "Entry" (if no experience required), "Junior" (if 1-2 years required), "Mid", "Senior", "Director/Executive" level (if mentioned, eitherwise Mid level is the default value)","minimum_level_of_education": "Integer. The minimum level of education required, that matches the ISCED definition. Not the level that will be considered as an advantage","employment_type": "[optional] Choose "Full-time", "Part-time", or something else. If not available the output is "Null".","employment_model": "[optional] Choose "On Site", "Remote", "Hybrid", or another kind of employment model - if mentioned, otherwise null."}}'
    + "Ensure to output the exact JSON format without any additional explanations!")
    
    output_experience_and_employment_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_experience_and_employment_classification))
    print("The experience and employment classification is: ", output_experience_and_employment_classification, "\n\n")


    #---------------------- Data preprocessing and extraction for Skills, Education Degree and Qualifications Data ----------------------#
    user_prompt_for_skills_and_qualifications = ("You will read the description of a job posting."
    + "Then you will summarize the skills and qualifications required for this job."
    + "Here is the job description: "+ str(db_job_data)
    + 'Your output must follow the JSON template: {"skills_and_qualifications":"Summary of all the skills and qualifications required for the job"}'
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_skills_and_qualifications = call_lmstudio_JSON(model, system_prompt, user_prompt_for_skills_and_qualifications)

    user_prompt_for_skills_classification = ("You will receive information about the skills required for a job posting."
    + "You will A) Read the information provided B) classify the skills required for this job."
    + "The job information is the following: "+ str(summarization_of_skills_and_qualifications)
    + "Do not include degrees and Certificates in this section."
    + "Your output must follow the JSON template:\n"
    + '{"skills": [{"skills_category": "Choose either "Soft Skill" or "Hard Skill".", "skills_name": "The name of each individual skill mentioned. The name must be brief, from 1 to 3 words. Each knowledge of languages, software or similar must be classified separately", "skills_type": "Technical skills, Programming Languages, Software, Professional or Drivers Licence, Personality Trait and others should be included here. Each skill must have an individual record in the list"}]}'
    + "Ensure to output the exact JSON format without any additional explanations!")

    output_skills_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_skills_classification))
    print("The skills classification is: ", output_skills_classification, "\n\n")
    
    user_prompt_for_degrees_and_qualifications_classification = ("You will receive information about the degrees and qualifications required for a job posting."
    + "You will A) Read the information provided B) classify the degrees and qualifications required for this job."
    + "The job information is the following: "+ str(summarization_of_skills_and_qualifications)
    + "Do not include skills or past work experience in this section."
    + "If multiple certifications, degrees or fields of study are mentioned, then they must have all be classified individually."
    + "Your output must follow the JSON template:\n"
    + '{"certifications": [{"certification_name": "Certification Name"}], "academic_degree": [{"academic_degree_name": "Degree Name", "academic_degree_field": "Degree Field"}]}'
    + "Ensure to output the exact JSON format without any additional explanations!")
    
    output_degrees_and_qualifications_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_degrees_and_qualifications_classification))
    print("The degrees and qualifications classification is: ", output_degrees_and_qualifications_classification, "\n\n")

    # Experience, Benefits, and Responsibilities
    user_prompt_for_summarization_of_experience_responsibilities_benefits = ("You will read the description of a job posting."
    + "Then you will summarize the experience required, the employee benefits and the employee responsibilities for this job."
    + "Here is the job description: "+ str(db_job_data)
    + "Your output must follow the JSON template: {'experience_and_responsibilities':'Summary of all the experience required, benefits and responsibilities from the job text provided'}"
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_experience_responsibilities_benefits = call_lmstudio_JSON(model, system_prompt, user_prompt_for_summarization_of_experience_responsibilities_benefits)
    
    user_prompt_for_experience_responsibilities_benefits_classification = ("You will receive information about the benefits of a job posting."
    + "You will A) Read the information provided B) classify the benefits of this job."
    + "The job information is the following: "+ str(summarization_of_experience_responsibilities_benefits)
    + "Your output must follow the JSON template:\n"
    + '{"experience": {"experience_required": The minimum years of experience required as a boolean value - if experience is required or not, "years_of_experience": The minimum years of experience required as an integer - if mentioned}, "benefits": [{"benefit_name": "The description of the benefit mentioned in the job listing. VERY brief description, 1 to 4 words."}], "responsibilities": [{"responsibility_name": "The description of the responsibility mentioned in the job listing. VERY brief description, 1 to 4 words."}]}'
    + "Ensure to output the exact JSON format without any additional explanations!")
    
    output_benefits_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_experience_responsibilities_benefits_classification))
    print("The benefits classification is: ", output_benefits_classification, "\n\n")   

    # Combine all JSON outputs into a single JSON object
    final_output_data_extracted_classified = {"job_reference": db_job_all_data['job_reference'], "job_description": db_job_data[1], **output_industry_classification, **output_job_title, **output_ISCO_classification, **output_experience_and_employment_classification, **output_skills_classification, **output_degrees_and_qualifications_classification, **output_benefits_classification}

    # print(final_output_data_extractedclassified)
    
    with open(f'final_output_data_extractedclassified_test.json', 'w', encoding='utf-8') as f:
        json.dump(final_output_data_extracted_classified, f, ensure_ascii=False, indent=4)

    return final_output_data_extracted_classified

#  -----------------   Testing the job data extraction and classification   ----------------- #
final_output_data_extracted_classified = job_data_preprocessing_extraction_classification("lmstudio_model", db_job_data)
# final_output_data_extracted_classified = open_JSON_files("final_output_data_extractedclassified_test.json")

validate_job_listing(final_output_data_extracted_classified)

