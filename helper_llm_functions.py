import os
from dotenv import load_dotenv
from groq import Groq
from helpers_sqldb import get_jobs_not_imported_to_neo4j

load_dotenv(override=True)


########################   VARIABLES   ########################
# Credentials for GROQ API
Groqllm = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# Available open-source models for Groq LLM: "llama3-70b-8192", "mixtral-8x7b-32768", "gemma-7b-it" and "llama3-8b-8192"
llama8B, llama70B, mixtral_8x7b = "llama3-8b-8192", "llama3-70b-8192", "mixtral-8x7b-32768"
llm_to_be_used = llama70B

########################   HELPER FUNTIONS   ########################
# Function to open the system file. Files likes user or system prompts and instructions.
def open_prompt_files(file):
    with open(file, 'r', encoding='utf-8') as f:
        promt_data = f.read()
    return promt_data

# Function to call Groq LLM
def call_groq_JSON(model, system_prompt, user_prompt_for_parsing):
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    chat_completion = Groqllm.chat.completions.create(
        messages=[{"role": "system","content": f"{system_prompt}"},{"role": "user","content": f"{user_prompt_for_parsing}"}],
        model=model,
        # models= "llama3-70b-8192", "mixtral-8x7b-32768", "gemma-7b-it" and "llama3-8b-8192"
        temperature=0,
        stream=False, # Streaming is not supported in JSON mode
        response_format={"type": "json_object"}, # Enable JSON mode by setting the response format
        max_tokens=8192)
    try:
        answer_text = str(chat_completion.choices[0].message.content)
        # print(answer_text)
    except Exception as e:
        print(e)
        answer_text = f"Error: {e} response from AI model."
    # print(answer_text) #for debugging
    return answer_text

# Function to call Groq LLM
def call_groq_nonJSON(model, system_prompt, user_prompt_for_parsing):
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    chat_completion = Groqllm.chat.completions.create(
        messages=[{"role": "system","content": f"{system_prompt}"},{"role": "user","content": f"{user_prompt_for_parsing}"}],
        model=model,
        # models= "llama3-70b-8192", "mixtral-8x7b-32768", "gemma-7b-it" and "llama3-8b-8192"
        temperature=0,
        stream=False, # Streaming is not supported in JSON mode
        # response_format={"type": "json_object"}, # Enable JSON mode by setting the response format
        max_tokens=8192)
    try:
        answer_text = str(chat_completion.choices[0].message.content)
        # print(answer_text)
    except Exception as e:
        print(e)
        answer_text = f"Error: {e} response from AI model."
    # print(answer_text) #for debugging
    return answer_text


########################   System and User Prompts for Data Extraction and Structuring    ########################
# The data to be extracted from a job listing in the PG DB that has not been imported to the Neo4j DB yet.
List_job_listing_not_imported_to_neo4j = get_jobs_not_imported_to_neo4j()
job_listing_not_imported_description = str(List_job_listing_not_imported_to_neo4j[635]['job_description'])

# The system prompt to be used for the LLM model
extract_system_prompt = open_prompt_files("data\prompts\system_prompt_extract_data.txt")

### The user prompt for parsing the job listing text
parsed_industry_data = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_job_industry.json") + "\n"
    + f" *** The job listing text is the following *** :\n" + job_listing_not_imported_description + "Focus on extracting the industry data of a job listing!" + "\n"
    + f" *** The industry standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_NACE.txt") + "\n"
    + f" Do not forget to extract the data in a JSON format!" + "\n")

parsed_main_data = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_main_job_data.json") + "\n"
    + f" *** The job listing text is the following *** :\n" + job_listing_not_imported_description + "Focus on extracting the main data of a job listing!" + "\n"
    + f" *** The ISCO standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCO.txt") + f"Ensure that you use the correct ISCO code!" + "\n"
    + f" *** The ISCED standards to choose from are these: *** :\n" + open_prompt_files(r"data\prompts\standard_ISCED.txt") + f"Ensure that you use the correct ISCED code!" + "\n"
    + f" Do not forget to extract the data in a JSON format!" + "\n")

parsed_skills_and_qualifications = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following. *** :\n" + open_prompt_files(r"data\prompts\json_template_skills.json") + "\n"
    + f" *** The job listing text is the following *** :\n" + job_listing_not_imported_description + f"Focus on extracting the skills and qualifications of a person!"
    + f" Do not forget to extract the data in a JSON format!" + "\n")

parsed_experience_and_responsibilities = (f" *** INSTRUCTIONS: ***\n" + open_prompt_files(r"data\prompts\user_prompt_extract_data.txt") + "\n"
    + f" *** The JSON template is the following *** :\n" + open_prompt_files(r"data\prompts\json_template_experience_benefits.json") + "\n"
    + f" *** The job listing text is the following *** :\n" + job_listing_not_imported_description
    + f" Focus on extracting the benefits of a job listing and experience and responsibilities of a person!"
    + f" Do not forget to extract the data in a JSON format!" + "\n")

# Extraction of data in batches, to increase the accuracy of the extracted data:
prompts = [parsed_industry_data, parsed_main_data, parsed_skills_and_qualifications, parsed_experience_and_responsibilities]

########################   System and User Prompts for Graph DB Cypher Code    ########################

