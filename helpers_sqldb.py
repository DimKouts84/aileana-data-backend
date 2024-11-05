import psycopg2, os, json, requests
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv(override=True)

#  -----------------     Variables    ----------------- #
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

# URL to call the embedding model
ollama_embed_url = os.getenv("OLLAMA_EMBEDD_URL")
# Ebmedding models (locall)
bge_m3, nomic_embed_text = 'bge-m3', 'nomic-embed-text'

#  -----------------     Testing    ----------------- #
# Test PGDB connection
def test_db_conn():
    try:
        cur, conn = connect_pg_conn(host, database, username, password)
        print("Connection to PostgreSQL DB successful")
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL DB: {e}")
    finally:
        cur.close()
        conn.close()

# Test NEO4J connection
def test_neo4j_connection(driver):
    with driver.session() as session:
        result = session.run("RETURN 1")
        single_result = result.single()[0] == 1
    driver.close()
    return single_result
# is_connected = test_neo4j_connection(driver)
# print(f"Connected to Neo4j: {is_connected}")

#  ----------------- HELPER SQLDB FUNTIONS ----------------- #
# Loop through the JOBS that are scraped and saved into the JSON file, then import them.
def insert_data_to_db(data):
    cur, conn = connect_pg_conn(host, database, username, password)
    rows_to_insert = []

    for item in data:
        row = {
            'title': item['Job Listing Title'],
            'reference': item['Job Listing Details Reference'],
            'company_name': item['Job Listing Company Name'],
            'listing_url': item['Listing URL']
        }
        rows_to_insert.append(row)

    for row in rows_to_insert:
        try:
            row['country'] = 'Cyprus'
            cur.execute("""
                INSERT INTO job_listings (title, reference, company_name, listing_url, country)
                VALUES (%(title)s, %(reference)s, %(company_name)s, %(listing_url)s, %(country)s)
            """, row)
            conn.commit()  # Commit after each insertion
        except psycopg2.Error as e:
            print(f"Error inserting row: {e} - Row: {row}")
            conn.rollback()  # Rollback if an error occurs

    cur.close()
    conn.close()


# Update job_listings table with Job Description
def update_job_description_data(job_description, reference):
    # DB connection
    cur, conn = connect_pg_conn(host, database, username, password)
    
    # Update job_listings table with Job Description
    cur.execute("""
        UPDATE job_listings
        SET job_description = %s, created_date = CURRENT_TIMESTAMP
        WHERE reference = %s
    """, (job_description, reference))
    # Commit changes
    conn.commit()
    # Close cursor and connection
    cur.close()
    conn.close()

# Make a list of all the reference IDs in the database
def get_list_with_ref_id():
    cur, conn = connect_pg_conn(host, database, username, password)
    cur.execute("""
        SELECT reference FROM job_listings
    """)
    list_of_ref_ids = [row[0] for row in cur.fetchall()]
    # print(get_list_with_ref_id())
    conn.close()
    return list_of_ref_ids


# Get all data from jobs if the 'imported' column is NOT True.
# Return a list with the following JOB data in JSON format: {"job_title", "job_reference", "job_description"}
def get_jobs_not_imported_to_neo4j():
    cur, conn = connect_pg_conn(host, database, username, password)
    cur.execute("""
        SELECT title, reference, job_description FROM job_listings WHERE imported IS NULL
    """)
    list_of_jobs = [{"job_title": row[0], "job_reference": row[1], "job_description": row[2]} for row in cur.fetchall()]
    conn.close()
    return list_of_jobs


# Update a job listing in the database as imported to Neo4j based on the `job reference`
def update_job_as_imported(job_reference):
    cur, conn = connect_pg_conn(host, database, username, password)
    cur.execute("""
        UPDATE job_listings
        SET imported = TRUE
        WHERE reference = %s
    """, (job_reference,))
    conn.commit()
    conn.close()
    

#  ----------------- HELPER GRAPHDB and EMBEDDING FUNTIONS ----------------- #
# Create a vector embedding from the text of a job description
def get_embedding(text, model):
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
    return response.json()['embeddings']


def create_relationship_skill_and_responsibilities_in_neo4j(structured_job_data):
    from helper_llm_main import call_groq_JSON
    # Extract the skills and responsibilities from the JSON file as a JSON object (not a list)
    skills = structured_job_data['skills']
    responsibilities = structured_job_data['responsibilities']
    # skills = list_job_listing_not_imported_to_neo4j.get("skills", [])
    # responsibilities = list_job_listing_not_imported_to_neo4j.get("responsibilities", [])

    # print("Skills --> \n", skills, f"\n\n-------------------\n\nResponsibilities --> \n", responsibilities)

    # 2) The prompts for the LLM to decide about the relationships
    system_prompt = ('You are an expert graph database developer with great knowledge of Cypher and for Neo4j more specifically'
                    + 'You are eager to help with any question. You ONLY responde with accuraty and precision. Your job depends on the accuracy of the cypher code you provide.')

    user_prompt = (f"*** INSTRUCTIONS: ***: You will receive job listing data which includes the *skills* and *responsibilities* required for this job"
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
                + f"Now read you can start writing the Cypher query if relationships found. Otherwhise respond with 'NONE'. \n"
                + f"Return the Cypher query as one string inside a JSON output.\n"
                + f"Example JSON output template: {{ 'cypher_query': 'the cypher query as one string'}}\n"
                )

  
    cypher_for_skill_and_responsibility_relationships = call_groq_JSON("llama3-70b-8192", system_prompt, user_prompt)
    # Parse the JSON string to a dictionary if it's not already one
    cypher_query_for_relationships = json.loads(cypher_for_skill_and_responsibility_relationships)
        
    # print(cypher_for_skill_and_responsibility_relationships)
    return cypher_query_for_relationships['cypher_query']


# Function to create nodes and relationships
def import_job_data_to_neo4j(session, job_data, job_reference, country, job_description_text):
    # Create INDUSTRY node
    industry_query = """
    MERGE (i:INDUSTRY {industry_name: $industry_name})
    ON CREATE SET i.standardized_industry_name = $standardized_industry_name
    """
    session.run(industry_query, industry_name=job_data['industry']['industry_name'], standardized_industry_name=job_data['industry']['NACE_standardized_name'])

    # Create JOB node
    job_query = """
    CREATE (j:JOB {job_title: $job_title, job_reference: $job_reference, 
                standardized_occupation: $standardized_occupation, job_seniority: $job_seniority, 
                minimum_level_of_education: $minimum_level_of_education, employment_type: $employment_type, 
                employment_model: $employment_model, country: $country, job_description: $job_description_text, embedding: NULL})
    """
    
    ##########################################################################################################################
    ################################## PREPEI NA TA FTIAKSO AUTA !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ##########################################################################################################################
    
    job_params = {
        'job_title': job_data['job_title'],
        'job_reference': job_reference,
        'standardized_occupation': job_data['isco_name'],
        # 'standardized_occupation_code': job_data['job']['standardized_occupation']['isco_code'],
        'job_seniority': job_data['occuation_details']['job_seniority'],
        'minimum_level_of_education': job_data['occuation_details']['minimum_level_of_education'],
        'employment_type': job_data['occuation_details'].get('employment_type', None),
        'employment_model': job_data['occuation_details'].get('employment_model', None),
        'country': country,
        'job_description_text': job_description_text
    }
    session.run(job_query, job_params)

    # Create relationship between INDUSTRY and JOB
    industry_job_rel_query = """
    MATCH (i:INDUSTRY {industry_name: $industry_name}), (j:JOB {job_title: $job_title})
    MERGE (i)-[:POSTS]->(j)
    """
    session.run(industry_job_rel_query, industry_name=job_data['industry']['industry_name'], job_title=job_data['job_title'])


    # Create SKILL nodes and relationships
    for skill in job_data['skills']:
        skill_query = """
        MERGE (s:SKILL {skill_name: $skill_name})
        ON CREATE SET s.skill_category = $skill_category, s.skill_type = $skill_type
        """
        session.run(skill_query, skill_name=skill['skills_name'], skill_category=skill['skills_category'], skill_type=skill['skills_type'])

        job_skill_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (s:SKILL {skill_name: $skill_name})
        MERGE (j)-[:NEEDS]->(s)
        """
        session.run(job_skill_rel_query, job_title=job_data['job_title'], skill_name=skill['skills_name'])

    # Create EXPERIENCE node and relationship
    experience_query = """
    MERGE (e:EXPERIENCE {minimum_years: $minimum_years})
    ON CREATE SET e.years_required = $years_required
    """
    session.run(experience_query, minimum_years=job_data['experience']['years_of_experience'], years_required=job_data['experience']['experience_required'])

    job_experience_rel_query = """
    MATCH (j:JOB {job_title: $job_title}), (e:EXPERIENCE {minimum_years: $minimum_years})
    MERGE (j)-[:REQUIRES]->(e)
    """
    session.run(job_experience_rel_query, job_title=job_data['job_title'], minimum_years=job_data['experience']['years_of_experience'])

    # Create BENEFIT nodes and relationships
    for benefit in job_data['benefits']:
        benefit_query = """
        MERGE (b:BENEFIT {benefit_name: $benefit_name})
        """
        session.run(benefit_query, benefit_name=benefit['benefit_name'])

        job_benefit_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (b:BENEFIT {benefit_name: $benefit_name})
        MERGE (j)-[:OFFERS]->(b)
        """
        session.run(job_benefit_rel_query, job_title=job_data['job_title'], benefit_name=benefit['benefit_name'])

    # Create RESPONSIBILITY nodes and relationships
    for responsibility in job_data['responsibilities']:
        responsibility_query = """
        MERGE (r:RESPONSIBILITY {description: $description})
        """
        session.run(responsibility_query, description=responsibility['responsibility_name'])

        job_responsibility_rel_query = """
        MATCH (j:JOB {job_title: $job_title}), (r:RESPONSIBILITY {description: $description})
        MERGE (j)-[:HAS]->(r)
        """
        session.run(job_responsibility_rel_query, job_title=job_data['job_title'], description=responsibility['responsibility_name']) 
    
    
    # Now we create the relationship between the SKILLS and RESPONSIBILITIES and write the Cypher query to insert the data into a Neo4j database.
    # for relationship in create_relationship_skill_and_responsibilities_in_neo4j(job_data):
    #     session.run(relationship)
    
    # Update the PostgreSQL database to mark the job as imported to Neo4j and close the connection
    update_job_as_imported(job_reference)
    driver.close()



# !!!! NUKE the neo4J database, delete all nodes and relationships in the database.
#  !!!!!!!!!!!!     CAUTION    !!!!!!!!!!!!
def nuke_neo4j_db():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run("MATCH (n) RETURN n")
        print("~~~~~~~~ Neo4j database has been N_U_K_E_D !!!!!")
    driver.close()

# ----------------- Testing and debugging ----------------- #
# print(get_embedding("This is a test"))
# nuke_neo4j_db()