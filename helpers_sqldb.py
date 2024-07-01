import psycopg2, os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv(override=True)

########################   POPSTGRESQL   ########################
# Postgres connection settings
# PostgreSQL DB Credentials
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

# Test DB connection
def test_db_conn():
    try:
        cur, conn = connect_pg_conn(host, database, username, password)
        print("Connection to PostgreSQL DB successful")
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL DB: {e}")
    finally:
        cur.close()
        conn.close()

########################   NEO4J GRAPH DB   ########################
# NEO4J GRAPH DB Credentials
neo4j_url = os.getenv("NEO4J_CONNECTION_URL")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

# Connect to the neo4j database
driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))


########################   GRAPH DB TESTING   ########################
def test_neo4j_connection(driver):
    with driver.session() as session:
        result = session.run("RETURN 1")
        single_result = result.single()[0] == 1
    driver.close()
    return single_result

# is_connected = test_neo4j_connection(driver)
# print(f"Connected to Neo4j: {is_connected}")


# loop through the JOBS that are scraped and saved into the JSON file, then import them.
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
        SET job_description = %s
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
# Reurn a list with the following JOB data in JSON format: {"Title", "Reference", "Job Description"}
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
    
# Nuke the neo4J database, delete all nodes and relationships and also reset the auto increment ID
def nuke_neo4j_db():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run("MATCH (n) RETURN n")
        print("~~~~~~~~ Neo4j database has been N_U_K_E_D !!!!!")
    driver.close()

nuke_neo4j_db()

