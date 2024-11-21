""" ALL THE IMPORTS """
# Imports of necessary libraries
import os, json, time, random, logging, datetime, psycopg2, requests, time
from dotenv import load_dotenv
from neo4j import GraphDatabase
# from groq import Groq  ~~~~~~~ UNINSTALL THIS PACKAGE ~~~~~~~
from pydantic import BaseModel, ValidationError
from typing import List, Optional

# Imports from other .py scripts
from helpers_sqldb import get_jobs_not_imported_to_neo4j, import_job_data_to_neo4j, nuke_neo4j_db, reset_imported_status

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

lmstudio_embeddings_url = os.getenv("LM_STUDIO_EMBEDDINGS_URL")
lmstudio_chat_completions_url = os.getenv("LM_STUDIO_COMPLETIONS_URL")
lmstudio_model = "qwen2.5-14b-instruct"
lmstudio_embedding_model = "text-embedding-bge-m3"

# Confifgure the logger and the timestamp
timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logging.basicConfig(filename='error_log.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#  -----------------   Pydantic Models for Data Validation    ----------------- #

class industry(BaseModel):
    industry_name: str
    NACE_standardized_name: str

class occupation_details(BaseModel):
    job_seniority: str
    minimum_level_of_education: int
    employment_type: Optional[str] = None  # Assign default value
    employment_model: Optional[str] = None  # Assign default value

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
    years_of_experience: Optional[int] = None  # Assign default value
    
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
    occupation_details: occupation_details
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
                "content": f"{user_prompt_for_parsing}"
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


#---------- Function to call Local LLM using the LMStudio API in JSON mode (same request as OPENAI) ----------#
def call_lmstudio_JSON(model, system_prompt, user_prompt_for_parsing):
    # Note for Models that worked well, especially with the JSON mode:: 
    ## Models with the best quality of output:  lmstudio-community/Qwen2.5-14B-Instruct-Q4_K_M.gguf,  lmstudio-community/Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf
    ## Models that worked ok: MaziyarPanahi/Qwen2.5-7B-Instruct-Uncensored.Q5_K_S.gguf, bartowski/Llama-3.2-3B-Instruct-f16.gguf
    
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


###  -----------------  System & User Prompts for Data Extraction, Structuring and Classification  ----------------- ###
# A function that takes the extracted data from two different LLM models and uses a third to define the correct output
def job_data_preprocessing_extraction_classification(model, db_job_data):
    # Alternatively Ollama can be used by changing the function call to call_ollama_JSON. LMStudio does not specific parameters for the model, just have a dummy model name.
    # print(f"Job description -->\n{db_job_data[0]}\n{db_job_data[1]}\n\n")
    
    system_prompt = open_prompt_files("data/prompts/system_prompt_extract_data.txt")

    #---------------------- Data preprocessing and extraction for Industry Data ----------------------#
    user_prompt_industry_summarization = ("You will read the description of a job posting."
    + "Then you will make a small description of company, what the company does and the industry this company operates."
    + "Here is the job description: "+ str(db_job_data)
    + "Your output must follow the JSON template: {'industry_summarization':'The description of the company and the industry this company operates in'}"
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_industry = call_lmstudio_JSON(model, system_prompt, user_prompt_industry_summarization)
    
    NACE_standardized_industry_title = [title.strip().strip('"').strip('\n') for title in open_prompt_files(r'data\prompts\standard_NACE.txt').strip('[]').split(',\n')]
    # print("NACE Standardized Industry Titles: ", NACE_standardized_industry_title,"\n")
    user_prompt_industry_data_classification = (f" Read the information provided for a company and the industry it operates in. "
    + "You will have to provide the the NACE industry title, from the list provided below."
    + "The NACE Standards Clasicifation List is here: "+ str(NACE_standardized_industry_title  )  
    + f" *** The company information is the following *** {summarization_industry}:\n"
    + 'Your outpout must follow the JSON template:'
    + '{"industry": {"industry_name":"A title of the industry","NACE_standardized_name":"The NACE title from the list that matches the company industry"}}'
    + "Ensure to output EXACTLY the JSON format without any additional explanations!")
    
    # Read the NACE classification and loop until the correct classification is made based on the job description
    while True:
        output_industry_classification_lvl_I = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_data_classification))
        
        if output_industry_classification_lvl_I['industry']['NACE_standardized_name'] in NACE_standardized_industry_title:
            print("The NACE Level I classification is: ", output_industry_classification_lvl_I, "\n\n")
            break
        else:
            print(f"The NACE Level I classification {output_industry_classification_lvl_I['industry']['NACE_standardized_name']} is incorrect. Please classify the NACE industry again.")
            # output_industry_classification_lvl_I = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_data_classification))
            continue
        
    #---------------------- Data preprocessing and extraction for Industry Subcategory Data ----------------------#
    NACE_standardized_subcategory_list = json.loads(open_prompt_files(r'data\prompts\NACE_Classification_Tree.json')) # [output_industry_classification_lvl_I['industry']['NACE_standardized_name']]
    NACE_standardized_subcategories = NACE_standardized_subcategory_list[output_industry_classification_lvl_I['industry']['NACE_standardized_name']]


    user_prompt_industry_subcategory_classification = (f" Read the information provided for a company and the industry it operates in. "
    + "You will have to provide the the NACE industry title, from the list provided below."
    + "The NACE Standards Clasicifation List is here: "+ str(NACE_standardized_subcategories)
    + f" *** The company information is the following *** {summarization_industry}:\n"
    + 'Your outpout must follow the JSON template:'
    + '{"industry": {"industry_name":"A title of the industry","NACE_standardized_name":"The NACE title from the list that matches the company industry"}}'
    + "Ensure to output EXACTLY the JSON format without any additional explanations!")

    # Read the NACE classification with subcategories and loop until the correct classification of the subcategory is made based on the job description
    while True:
        output_industry_classification_lvl_II = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_subcategory_classification))
        
        if output_industry_classification_lvl_II['industry']['NACE_standardized_name'] in NACE_standardized_subcategories:
            print("The NACE Level II classification is: ", output_industry_classification_lvl_II, "\n\n")
            break
        else:
            print(f"The NACE Level II classification {output_industry_classification_lvl_II['industry']['NACE_standardized_name']} is incorrect. Please classify the NACE industry again.")
            # output_industry_classification_lvl_II = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_industry_subcategory_classification))
            continue

    #---------------------- Data preprocessing and extraction for Main Job Data ----------------------#
    user_prompt_for_job_title_summarization = ("You will read the description of a job posting. "
    +"Then you will summarize the description of the job in a sentence."
    +"Here is the job description: "+ str(db_job_data)
    +'Your output must follow the JSON template: {"job_title_description":"The job title summarization in a sentence"}'
    +"Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_job_title = call_lmstudio_JSON(model, system_prompt, user_prompt_for_job_title_summarization)
    # print("The job title summarization is: ", summarization_of_job_title, "\n")
    
    user_prompt_for_job_title = ("You will receive information about the job title of a job posting."
    + "You will output ONLY the title of the job. Do not include the company name or any other information, just the job title itself."
    + "The job information is the following: "+ str(summarization_of_job_title)
    + 'Your output must follow the JSON template: {"job_title":"The job title"}'
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    output_job_title = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_job_title))
    # print("The job title is: ", output_job_title, "\n")
    
    # ISCO_standardized_occupation_title = open_ISCO_file(r'data\prompts\standard_ISCO.txt')
    ISCO_standardized_occupation_title = [title.strip('"') for title in open_prompt_files(r'data\prompts\standard_ISCO.txt').strip('[]').split(',\n')]
    user_prompt_for_ISCO_classification = ("You will receive job information about the job title of a job posting."
    +"You will have to classify the ISCO title based on the job description."
    +"Step A: Read all ISCO titles"
    +"Step B: Think step by step and choose the ISCO title that matches the job description!\n" 
    +"Here is a list of ISCO titles is:"+ str(ISCO_standardized_occupation_title)
    +"The job title is the following: "+ str(summarization_of_job_title) +"\n"
    +'Your output must follow the JSON template: {"isco_name":"The ISCO title from the provided list"}'
    +"Ensure that you choose the correct ISCO title in JSON format without any additional explanations."
    +"Ensure that you choose an ISCO title that matches the job job description.")
    
    while True:
        output_ISCO_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_ISCO_classification))
        if output_ISCO_classification['isco_name'] in ISCO_standardized_occupation_title:
            print("The ISCO classification is: ", output_ISCO_classification['isco_name'], "\n")
            break
        else:
            print(f"The ISCO classification {output_ISCO_classification['isco_name']} is incorrect. Retrying...")
            time.sleep(1)  # Optional: Add small delay between retries
            continue

    #---------------------- Data preprocessing and extraction for Experience and Employment Data ----------------------#
    user_prompt_for_experience_and_employment = ("You will read the description of a job posting."
    +"Then you will summarize, in one sentence: a) how many years experience is required for this job, b) what type of employment is offered (full time, part-time, remote, hybrid etc) c) the education level and degree required for this job."
    +'Your output must follow the JSON template: {"experience_and_employment":"Summarization of the minimum years of experience required, educational level or degrees required and the employment type. The output must be in one sentence"}'
    +"Ensure to output ONLY in JSON format without any additional explanations!"
    +"Here is the job description: "+ str(db_job_data))
    
    summarization_of_experience_and_employment = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_experience_and_employment))    
    
    ISCED_stabdardized_occupation_title = [open_prompt_files(r'data\prompts\standard_ISCED.txt')]
    user_prompt_for_experience_and_employment_classification = ("You will receive information about the experience required and employment type of a job posting."
    + "You will A) Read the information provided B) classify the job seniority C) classify the minimum level of education required based on ISCED"
    + "D) employment Type and E) employment model for this job.\n"
    + "The job information is the following: "+ str(summarization_of_experience_and_employment)
    + " *** The ISCED Standard to choose from are here: *** :" + str(ISCED_stabdardized_occupation_title)
    + "Your output must follow the JSON template:\n"
    + '{"occupation_details":{"job_seniority": " "Internship", "Entry" (if no experience required), "Junior" (if 1-2 years required), "Mid", "Senior", "Director/Executive" level (if mentioned, eitherwise Mid level is the default value)","minimum_level_of_education": "Integer. The minimum level of education required, that matches the ISCED definition. Not the level that will be considered as an advantage","employment_type": "[optional] Choose "Full-time", "Part-time", or something else. If not available the output is "Null".","employment_model": "[optional] Choose "On Site", "Remote", "Hybrid", or another kind of employment model - if mentioned, otherwise null."}}'
    + "Ensure to output the exact JSON format without any additional explanations!")
    
    output_experience_and_employment_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_experience_and_employment_classification))
    print("The experience and employment classification is: ", output_experience_and_employment_classification, "\n\n")


    #---------------------- Data preprocessing and extraction for Skills, Education Degree and Qualifications Data ----------------------#
    user_prompt_for_skills_and_qualifications_summarization = ("You will read the description of a job posting."
    + "Then you will summarize the skills and qualifications required for this job."
    + "Here is the job description: "+ str(db_job_data)
    + 'Your output must follow the JSON template: {"skills_and_qualifications":"Summary of the skills, types of skills and other requirements for the job"}'
    + "Ensure to output ONLY in JSON format without any additional explanations!")
    
    summarization_of_skills_and_qualifications = call_lmstudio_JSON(model, system_prompt, user_prompt_for_skills_and_qualifications_summarization)
    # print("The skills and qualifications summarization is: ", summarization_of_skills_and_qualifications, "\n\n")

    user_prompt_for_skills_classification = ("You will receive information about the skills required for a job posting."
    + "You will A) Read the information provided B) classify the skills required for this job."
    + "The job information is the following: "+ str(summarization_of_skills_and_qualifications)
    + "Do not include degrees and Certificates in this section."
    + "Your output must follow the JSON template:\n"
    + '{"skills": [{"skills_category": "Either `Soft Skill` or `Hard Skill`", "skills_name": "The name of each individual skill mentioned. The name must be brief, from 1 to 3 words. Each knowledge of languages, software or similar must be classified separately", "skills_type": "`Technical skills`, `Programming Languages`, `Software`, `Professional` or `Drivers Licence`, `Personality Trait` and others should be included here. Each skill must have an individual record in the list"}]}'
    + "Ensure to output the exact JSON format without any additional explanations!")

    output_skills_classification = json.loads(call_lmstudio_JSON(model, system_prompt, user_prompt_for_skills_classification))
    print("The skills classification is: ", output_skills_classification, "\n\n")
    
    user_prompt_for_degrees_and_qualifications_classification = ("You will receive information about the degrees and qualifications required for a job posting."
    + "You will A) Read the information provided B) classify the degrees and qualifications required for this job."
    + "The job information is the following: "+ str(summarization_of_skills_and_qualifications)
    + "Do not include skills or past work experience in this section."
    + "If multiple certifications, degrees or fields of study are mentioned, then they must have all be classified individually."
    + "Your output must follow the JSON template:\n"
    + '{"certifications": [{"certification_name": "Certification Name"}], "academic_degree": [{"academic_degree": "Title of Degree (e.g. Bachelors, Masters, PhD, etc)", "academic_degree_field": "Degree Field"}]}'
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
    final_output_data_extracted_classified = {
        "job_reference": db_job_data["job_reference"], 
        "job_description": db_job_data["job_description"], 
        **output_industry_classification_lvl_II, 
        **output_job_title, 
        **output_ISCO_classification, 
        **output_experience_and_employment_classification, 
        **output_skills_classification, 
        **output_degrees_and_qualifications_classification, 
        **output_benefits_classification}

    # print(final_output_data_extractedclassified)
    
    with open(f'final_output_data_extractedclassified_test.json', 'w', encoding='utf-8') as f:
        json.dump(final_output_data_extracted_classified, f, ensure_ascii=False, indent=4)

    return final_output_data_extracted_classified

""" ----------------- Job Data Processing and importing to Graph Database ----------------- """
def process_jobs_and_import_to_graphDB(driver, country):
    # Get all the jobs that are not imported to the Graph DB
    all_job_not_into_graphDB = get_jobs_not_imported_to_neo4j()
    
    # The system prompt to be used for the LLM model
    current_model = lmstudio_model  # For Ollama or OpenAI a specific model named must be passed. for LMStudio is not necessary.

    with driver.session() as session:
        # Loop through all the jobs that are not imported to the Graph DB and import them
        for job_data in all_job_not_into_graphDB:
            print(job_data, "\n")
            # print(f"Job with {job_data['job_reference']}")
            # print(f"Job description: {job_data['job_description']}\n")
            retry_count, max_retries = 0, 10
            while retry_count < max_retries:
                try:
                    job_data = job_data_preprocessing_extraction_classification(current_model, job_data)
                    
                    # VALIDATE THE JOB DATA
                    if validate_job_listing(job_data):
                        print("Job data is valid!\n")
                    else:
                        print("Job data is invalid!\n")
                        break
                    
                    # Import the job data to the Neo4j database if the process was successful
                    import_job_data_to_neo4j(session, job_data, job_data['job_reference'], country, job_data['job_description'])
                    break  # Break out of the retry loop if successful
                except Exception as e:
                    error_message = str(e)
                    print(f"Error Message: {error_message}\n !")
                    logging.error(f"Error processing job {job_data['job_reference']} | {error_message}")
                    retry_count += 1

            if retry_count == max_retries:
                print(f"Failed to process job {job_data['job_reference']} all times.")



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
def create_ollama_embeddings_data(text, model):
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
    return response.json()['embeddings']


def create_lmstudio_embeddings_data(text, model):
    # LMStudio API does not require a specific model name
    url = lmstudio_embeddings_url
    
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
    # save the embedding into a JSON file
    embedding = response.json()['data'][0]['embedding']
    return embedding

class LocalLLMError(Exception):
    pass

def create_ollama_embeddings_data_with_retries(text, model, retries=5):
    for attempt in range(retries):
        try:
            return create_ollama_embeddings_data(text, model)
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            # time.sleep(20)  # Optional: wait a bit before retrying
    raise LocalLLMError("Local LLM is not working after multiple attempts.") 


def create_lmstudio_embeddings_data_with_retries(text, model, retries=5):
    for attempt in range(retries):
        try:
            return create_lmstudio_embeddings_data(text, model)
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            # time.sleep(20)  # Optional: wait a bit before retrying
    raise LocalLLMError("LMStudio is not working after multiple attempts.")


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


def job_rag_pipeline(driver):
    # Query for jobs that do not have embeddings in the Neo4jDB
    query = """
    MATCH (j:JOB)
    WHERE (j.embedding) IS NULL
    RETURN j.job_reference AS job_reference, j.job_description AS job_description
    """
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            job_reference = record["job_reference"]
            print(f"Processing job {job_reference}...")
            # Create the embeddings with LMStudio
            embedding = create_lmstudio_embeddings_data_with_retries(record, lmstudio_embedding_model)
            # ''' ~ OR ~ '''
            # Create the embeddings with Ollama
            # embedding = create_ollama_embeddings_data_with_retries(record, "bge-m3:latest")
            
            # Add the embeddings to the Neo4j DB
            add_embedding_to_NEO4J_job(embedding, job_reference)
            # Add the embeddings to the PostgreSQL DB
            add_embedding_to_PG_job(embedding, job_reference)
            print(f"Embeddings for {job_reference} added to the Neo4j and PostgreSQL DBs.")
    return 


""" ----------------- TESTING STUFF ----------------- """
# nuke_neo4j_db()
# reset_imported_status()

# Test job_data_preprocessing_extraction_classification() function
# Get a random job data from the PostgreSQL DB

# def test_job_data_stuff():
#     job_data = get_jobs_not_imported_to_neo4j()[12533]
#     print(f"Job data: {job_data}\n")    
    
#     job_data_preprocessing_extraction_classification(lmstudio_model, job_data)
    
#     process_jobs_and_import_to_graphDB(driver, country="Cyprus")
#     # job_data = get_node_data_from_neo4J_job(driver, "REF. NUM: 250983")
#     print("JOB NOD DATA---->\n", get_node_data_from_neo4J_job(driver, "REF. NUM: 250983"), "\n")
#     print("JOB DATA EMBEDDING ---->", create_lmstudio_embeddings_data(str(job_data), "text-embedding-bge-m3"), "\n")

# test_job_data_stuff()
# print(create_lmstudio_embeddings_data(str(job_data), "text-embedding-bge-m3"), type(create_lmstudio_embeddings_data(str(job_data), "text-embedding-bge-m3")))
# print(create_ollama_embeddings_data(str(job_data), "bge-m3:latest"), type(create_ollama_embeddings_data(str(job_data), "bge-m3:latest")))

# job_rag_pipeline(driver)

