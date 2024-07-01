from helpers_sqldb import get_jobs_not_imported_to_neo4j
from helper_llm_functions import call_groq_nonJSON, call_groq_JSON
from neo4j import GraphDatabase
import json, os
from dotenv import load_dotenv

load_dotenv(override=True)

########################     Variables    ########################
# NEO4J GRAPH DB Credentials
neo4j_url = os.getenv("NEO4J_CONNECTION_URL")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Connect to the neo4j database
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

# load json file
list_job_listing_not_imported_to_neo4j = get_jobs_not_imported_to_neo4j()[1163]


######################### # # # CYPHER SCRIPTS # # # # ########################
# Function to create nodes and relationships
def import_job_data_to_neo4j(session, data, job_reference, country, job_description_text):
    # Create INDUSTRY node
    industry_query = """
    MERGE (i:INDUSTRY {industry_name: $industry_name})
    ON CREATE SET i.standardized_industry_name = $standardized_industry_name
    """
    session.run(industry_query, industry_name=data['industry']['industry_name'], standardized_industry_name=data['industry']['NACE_industry_name'])

    # Create JOB node
    # Create JOB node
    job_query = """
    CREATE (j:JOB {job_title: $job_title, job_reference: $job_reference, standardized_occupation: $standardized_occupation, 
                standardized_occupation_code: $standardized_occupation_code, job_seniority: $job_seniority, 
                minimum_level_of_education: $minimum_level_of_education, employment_type: $employment_type, 
                employment_model: $employment_model, country: $country, job_description: $job_description_text})
    """
    
    job_params = {
        'job_title': data['job']['job_title'],
        'job_reference': job_reference,
        'standardized_occupation': data['job']['standardized_occupation']['isco_name'],
        'standardized_occupation_code': data['job']['standardized_occupation']['isco_code'],
        'job_seniority': data['job']['job_seniority'],
        'minimum_level_of_education': data['job']['minimum_level_of_education'],
        'employment_type': data['job'].get('employment_type', None),
        'employment_model': data['job'].get('employment_model', None),
        'country': country,
        'job_description_text': job_description_text
    }
    session.run(job_query, job_params)

    # Create relationship between INDUSTRY and JOB
    industry_job_rel_query = """
    MATCH (i:INDUSTRY {industry_name: $industry_name}), (j:JOB {job_title: $job_title})
    MERGE (i)-[:POSTS]->(j)
    """
    session.run(industry_job_rel_query, industry_name=data['industry']['industry_name'], job_title=data['job']['job_title'])

    # Create SKILL nodes and relationships
    for skill in data['skills']:
        skill_query = """
        MERGE (s:SKILL {skill_name: $skill_name})
        ON CREATE SET s.skill_category = $skill_category, s.skill_type = $skill_type
        """
        session.run(skill_query, skill_name=skill['skills_name'], skill_category=skill['skills_category'], skill_type=skill['skills_type'])

        job_skill_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (s:SKILL {skill_name: $skill_name})
        MERGE (j)-[:NEEDS]->(s)
        """
        session.run(job_skill_rel_query, job_title=data['job']['job_title'], skill_name=skill['skills_name'])

    # Create EXPERIENCE node and relationship
    experience_query = """
    MERGE (e:EXPERIENCE {minimum_years: $minimum_years})
    ON CREATE SET e.years_required = $years_required
    """
    session.run(experience_query, minimum_years=data['experience']['years_of_experience'], years_required=data['experience']['experience_required'])

    job_experience_rel_query = """
    MATCH (j:JOB {job_title: $job_title}), (e:EXPERIENCE {minimum_years: $minimum_years})
    MERGE (j)-[:REQUIRES]->(e)
    """
    session.run(job_experience_rel_query, job_title=data['job']['job_title'], minimum_years=data['experience']['years_of_experience'])

    # Create BENEFIT nodes and relationships
    for benefit in data['benefits']:
        benefit_query = """
        MERGE (b:BENEFIT {benefit_name: $benefit_name})
        """
        session.run(benefit_query, benefit_name=benefit['benefit_name'])

        job_benefit_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (b:BENEFIT {benefit_name: $benefit_name})
        MERGE (j)-[:OFFERS]->(b)
        """
        session.run(job_benefit_rel_query, job_title=data['job']['job_title'], benefit_name=benefit['benefit_name'])

    # Create RESPONSIBILITY nodes and relationships
    for responsibility in data['responsibilities']:
        responsibility_query = """
        MERGE (r:RESPONSIBILITY {description: $description})
        """
        session.run(responsibility_query, description=responsibility['responsibility_name'])

        job_responsibility_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (r:RESPONSIBILITY {description: $description})
        MERGE (j)-[:HAS]->(r)
        """
        session.run(job_responsibility_rel_query, job_title=data['job']['job_title'], description=responsibility['responsibility_name'])
    
    # print("########### Job data imported successfully! ###########")
    # Close the driver
    driver.close()



def create_relationship_skill_and_responsibilities_in_neo4j(session, job_data):
    # Extract the skills and responsibilities from the JSON file as a JSON object (not a list)
    skills = list_job_listing_not_imported_to_neo4j.get("skills", [])
    responsibilities = list_job_listing_not_imported_to_neo4j.get("responsibilities", [])

    # print("Skills --> \n", skills, f"\n\n-------------------\n\nResponsibilities --> \n", responsibilities)

    # 2) The prompts for the LLM to decide about the relationships
    system_prompt = ('You are an expert graph database developer with great knowledge of Cypher and for Neo4j more specifically'
                    + 'You are eager to help with any question. You ONLY responde with accuraty and precision. Your job depends on the accuracy of the cypher code you provide.')

    user_prompt = (f"*** GINSTRUCTIONS: ***: You will receive job listing data related to *skills* and *responsibilities*,"
                + f"Then will have to provide a Cypher query that creates relations between the 'SKILS' and 'RESPONSIBILITY' nodes."
                + f"We will use your Cypher query insert the data into a Neo4j database."
                + f"*** STEP BY STEP INSTRUCTIONS: ***:\n"
                + f"1. Read the JSON file of job post. \n"
                + f"2. Decide if there is a *SKILL* node that relates to a specific a *RESPONSIBILITY* node \n"
                + f"3. Now ,write a Cypher query. Which 'skills' are related to which 'responsibilities' \n"
                + f"Note: Asume that the nodes of Skills and Responsibilities are already in the database. \n\n"
                + f"Think critically and reflect on your thoughts."
                + f"If there is a relationship between a skill and a responsibility, you will write a Cypher query to insert the data into a Neo4j database. \n"
                + f"if there is no relationship between a skill and a responsibility DO NOT make a relationship. Your output, it this case, must be 'False' \n"
                + f"Your response must **ONLY** with the Cypher query, NOTHING else. \n\n"
                + f"Now read the data and start thinking about the relationships between the nodes:\n"
                + f"Skills data: \n" + ', ' + str(skills)
                + f"Responsibilities data: \n" + ', ' + str(responsibilities)
                + f"Here is a cypher query example: \n"
                + f'''MATCH (s:SKILL {{'skill_name: Python'}})-[r:RELATES_TO]->(r:RESPONSIBILITY {{'description': 'Developing software applications'}}) RETURN s, r \n\n'''
                + f"Now you can start writing the Cypher query if relationships found. Otherwhise respond with 'NONE'. \n"
                + f"Return the Cypher query as one string inside a JSON output.\n"
                + f"Example JSON output template: {{ 'cypher_query': 'the cyperquery as one string'}}\n"
                )

    # Call the LLM API to output a CYPHER 
    # Available open-source models for Groq LLM: "llama3-70b-8192", "mixtral-8x7b-32768", "gemma-7b-it" and "llama3-8b-8192"
    llama8B, llama70B, mixtral_8x7b = "llama3-8b-8192", "llama3-70b-8192", "mixtral-8x7b-32768"
    
    cypher_for_skill_and_responsibility_relatiosnhips = call_groq_JSON(llama70B, system_prompt, user_prompt)
    print(cypher_for_skill_and_responsibility_relatiosnhips)
    return cypher_for_skill_and_responsibility_relatiosnhips

# # Test the create_relationship_skill_and_responsibilities_in_neo4j function
# job_data = get_jobs_not_imported_to_neo4j()[654]
# # print(job_data)
# with driver.session() as session:
#     relationship_cypher_query = create_relationship_skill_and_responsibilities_in_neo4j(session, job_data)
#     # print(f"~~~~~~~~ Relationship cypher query created in Neo4j for company ~~{job_data['job_reference']}~~\n{relationship_cypher_query}")

