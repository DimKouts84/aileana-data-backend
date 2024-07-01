# Aileana

> **Navigating the job market one prompt at a time** .

<br>

**Aileana** is an advanced LLM chatbot with a comprehensive understanding of the job market, designed to help individuals navigate their career paths and keep professionals updated on the latest market trends.

Aileana excels at identifying and correlating  *jobs* ,  *skills* ,  *requirements* ,  *benefits* ,  *required experience* , and  *responsibilities* . Utilizing agentic workflows from state-of-the-art LLMs and Retrieval-Augmented Generation (RAG), she enhances accuracy and reliability with data sourced from recent and relevant information. The Knowledge Graph database allows the models to perform more detailed queries and deeper analysis than traditional vector embeddings in SQL databases.

---

## 💬 The Story

**Eleana** was nearly 18 years old, facing the critical decision of choosing her career and relevant academic studies.

She had a passion for Biology  *(or at least thought she did)* , but no one in her family or circle was familiar with this field, and the job prospects were uncertain 🤔.

Given my background in the health domain, she asked me if pursuing Biology was a good idea. My response was something like: " *If it is your passion, follow it.* "

However, this answer was *generic* and  *insincere* , to be honest. The truth was that *I had no idea.* This led me to ponder:

* Is it really good advice? Is 'liking' a subject the only criterion for choosing a field of study?
* What are the employment prospects after graduation?
* What related jobs are available, and what other skills are needed to qualify for these jobs?
* What benefits do such jobs usually offer?

As a data nerd 🤓, I realized the job market operates like any other market:

* Supply and demand are key driving forces.
* Higher qualifications/specializations generally lead to higher pay.

Only *data* can provide answers to these questions, not anecdotal experiences.

Thus,  ***Aileana*** , this project, was born.

<br>

---

## ⚡️Tech Stack

| Category                  | Technology                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Frontend**        | ![Streamlit](https://img.shields.io/badge/-Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)                                                                                                                                                                                                                                                                                                                                                                                              |
| **Backend**         | ![Python](https://img.shields.io/badge/-Python-3776AB?style=flat&logo=python&logoColor=white)                                                                                                                                                                                                                                                                                                                                                                                                       |
| **Databases**       | ![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white) ![Neo4j](https://img.shields.io/badge/-Neo4j-008CC1?style=flat&logo=neo4j&logoColor=white)                                                                                                                                                                                                                                                                                                  |
| **Web Scraping**    | ![Beautiful Soup](https://img.shields.io/badge/-Beautiful_Soup-FFD700?style=flat&logo=beautiful-soup&logoColor=black) ![Selenium](https://img.shields.io/badge/-Selenium-43B02A?style=flat&logo=selenium&logoColor=white)                                                                                                                                                                                                                                                                             |
| **Data Processing** | ![Pandas](https://img.shields.io/badge/-Pandas-150458?style=flat&logo=pandas&logoColor=white)                                                                                                                                                                                                                                                                                                                                                                                                       |
| **DevOps**          | ![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat&logo=docker&logoColor=white)                                                                                                                                                                                                                                                                                                                                                                                                       |
| **Testing**         | ![Pytest](https://img.shields.io/badge/-Pytest-0A9EDC?style=flat&logo=pytest&logoColor=white)                                                                                                                                                                                                                                                                                                                                                                                                       |
| **LLM Frameworks**  | ![Langchain](https://img.shields.io/badge/-Langchain-FF4B4B?style=flat&logo=langchain&logoColor=white) ![OpenAI](https://img.shields.io/badge/-OpenAI-02DE20?style=flat&logo=openai&logoColor=Green) ![Groq](https://img.shields.io/badge/-Groq-FFA200?style=flat&logo=groq&logoColor=Orange) ![Llama 3](https://img.shields.io/badge/-Llama_3-150458?style=flat&logo=llama3&logoColor=blue) ![Hugging Face](https://img.shields.io/badge/-Hugging_Face-FFD700?style=flat&logo=huggingface&logoColor=white) |

**Note**: Feel free to can use the `requirements.txt` to `pip install` all dependancies in the project environment.

</br>

---

## 📕 The Process

## 🔮 How the Magic Happens

Here is an overview of the main processes that take place to achieve the end result:

1. The system scrapes job listing websites for *(new)* jobs.
2. Translates all scraped listings into English  *(if not already in English 🇬🇧)* .
3. Stores all scraped listings in a `PostgreSQL` database.
4. The unstructured data is processed by the LLM to extract valuable information in a structured format.
5. Using the structured data, predefined `nodes`, `attributes`, and their `relationships` are stored in the `Neo4j` graph database.
6. Data cleaning for the node `labels` and `attributes`.
7. Creates vector `embeddings` and an `index` in the graph database for use in RAG.
8. `RAG` is used to ground a conversational LLM *(avoiding hallucinations)* to assist users with related questions.
9. In addition to written responses, LLMs create charts/plots visualizing data based on user prompts.

<br>

---

### **🕷️ Web Scraping**

To scrape job listings, I primarily used `Beautiful Soup 4` and `Selenium` to navigate through websites and extract each job listing. Job listing websites typically have straightforward, repetitive layouts, making information extraction easy-peasy.

<br>

---

### **💽 SQL Database**

After gathering and translating all the job data, it was stored in a `PostgreSQL` database. This database serves as a single source of truth before further processing the unstructured data included in the job descriptions. The database schema is simple, with just one table and lots of columns 😜.

<br>

---

### 🤖 **Knowledge Graph Database**

This was the most interesting part for a coding newbie like me.

Each job listing stored in the PostgreSQL database is parsed using an LLM to populate the Knowledge Graph database `Neo4J` based on the schema below.

After importing all jobs into the Knowledge Graph, another LLM process is used for rough data analysis and cleaning (e.g., removing duplicate entries or similar data in other nodes).

Finally, embeddings for each node and a vector index are created and updated after each new batch of scraped listings.

<br>

**International Standards:**

For more accurate data analysis, I decided to adopt a few International Standards:

* **NACE** (Nomenclature of Economic Activities) V2 - A European standard for classification of economic activities.
* **ISCED** (2011) - Levels of education - A framework for categorizing levels of education into seven levels.
* **ISCO-88** Occupation Titles - An International Standard Classification of Occupations that groups jobs into four levels of aggregation.

This will help with the accuracy of data analysis of different types of jobs posted by companies in various industries in correlation with their level of education and related skills.

<br>

**The Knowledge Graph schema**

Nodes:

**`INDUSTRY CATEGORY`**

* Label: `INDUSTRY_CATEGORY`
* Property Keys:
  * `industry_name`: The industry under which the company posted the listing.
  * `standardized_industry_name`: Standardized industry type based on NACE (Nomenclature of Economic Activities) V2.

**`JOB TITLE`**

* Label: `JOB_TITLE`
* Property Keys:
  * `job_title`: The job title as mentioned in the job listing.
  * `standardized_occupation`: The standardized occupation based on ISCO-88.
  * `job_seniority`: Internship, Entry, Junior, Mid, or Senior level (if mentioned).
  * `minimum_level_of_education`: The minimum level of education required, based on ISCED (2011).
  * `external_id`: The ID used by the job listing website (if applicable).
  * `employment_type` [optional]: Full-time, Part-time, etc. (if mentioned).
  * `employment_model` [optional]: On-site, Remote, Hybrid, or any other employment model (if mentioned).

**`SKILLS`**

* Label: `SKILL`
* Property Keys:
  * `skill_category`: Soft or Hard skill.
  * `skill_name`: Name of the skill.
  * `skill_type` [optional]: Academic Skill, Technical Skill, Knowledge of a Software tool, Professional Certification, Personality Attribute, Fluency in a Language, or any other skill.

**`BENEFITS`**

* Label: `BENEFIT`
* Property Keys:
  * `benefit_name`: Days of annual leave, Health Insurance (Private, Public, or both), Provident Fund, Amenities, or any other benefit.

**`EXPERIENCE`**

* Label: `EXPERIENCE`
* Property Keys:
  * `years_required`: Whether previous experience is required (boolean) (if mentioned).
  * `minimum_years` [optional]: The minimum number of years needed (integer) (if mentioned).

**`RESPONSIBILITIES`**

* Label: `RESPONSIBILITY`
* Property Keys:
  * `description`: A minimal summary of each responsibility requested in the job listing.

Node Relationships:

* `INDUSTRY_CATEGORY` |POSTS| `JOB_TITLE`
* `JOB_TITLE` |NEEDS| `SKILL`
* `JOB_TITLE` |REQUIRES| `EXPERIENCE`
* `JOB_TITLE` |OFFERS| `BENEFITS`
* `JOB_TITLE` |HAS| `RESPONSIBILITY`
* `RESPONSIBILITY` |RELATES_TO| `SKILL`

<br>

---

### 🖥️ **Front End**

I was intrigued by watching people build various data analysis projects with `Streamlit`, and it seemed easy enough for me. Another fun challenge was incorporating functionality for LLMs to plot charts for users based on their prompts.

<br>
Answering user queries with accuracy:

The frontend LLM has knowledge of the database schema and can query the database to access nodes relevant to the user's prompt.

Then the agent, with the help of RAG, can decide which nodes are most similar to the query and provide the necessary information or analysis accurately.

<br>

---

### **🧹 Data Cleaning**

Since LLMs are used to extract data, sometimes they hallucinate and may not follow instructions to categorize jobs and industries based on the standards (NACE, ISCO, etc.).

For this reason, I created a framework of AI agents that query specific data from the database and, using logic and reasoning, decide whether to keep or change it based on the instructions.

Automated data cleaning process example:

* **agent_db_analyst** : This agent queries the database for specific data (e.g., the ISCO occupation or NACE category) and rechecks if it is correct. Each decision is passed to `agent_manager`.
* **agent_manager** : The manager can validate the result and accept the decision of the `agent_db_analyst` or inform the `agent_db_analyst` to try again.
* **agent_validator** :
* **agentic_rules** :
* Each task can be repeated only three times; otherwise, it is logged, and no changes are made to the database.
* Each standard must be passed as a variable in the prompt.

Another automated data cleaning example:

* **agent_db_analyst** : Selects all benefits with "GYM" as a keyword and decides if the text will be "Free Gym Membership" or "Gym in Company Premises." Each decision is passed to `agent_manager`.
* **agent_manager** : The manager can validate the result and accept the decision of the `agent_db_analyst` or inform the `agent_db_analyst` to try again.
* **agent_validator** :
* **agentic_rules** : Each task can be repeated only three times; otherwise, it is logged, and no changes are made to the database.

<br>

---

## 🚀 Conclusions

### ⚡️The Tech

Imagine the dynamic duo of GROQ with its lightning-fast speeds 🚀💨 paired with the incredible reasoning prowess of LLama 3 70B. This powerhouse combination makes extracting key information from job listings a breeze! It was so fast and easy that I even used LLMs for simple tasks like text translations. After cross-checking the response from OpenAI's Chat-GPT4o, the results were surprisingly close, making it a no-brainer due to the cost ( and speed! ) difference.

While one database could suffice, I initially planned to use the PGVector add-on for PostgreSQL. However, after parsing a few thousand listings 😅, I realized leveraging LLMs (Large Language Models) for data extraction was a brilliant move for a data analysis project. This approach becomes even more exciting when combined with a Knowledge Graph database like Neo4J - it's a dream come true for my inner data geek 🔍!

After some research, I found that the embeddings model from OpenAI provided excellent results, though it came with a cost of vectorizing thousands of job listings ▿️.

<br>

---

### 💭A Few Thoughts

This project is my cool experiment to see if we can really put Large Language Models (LLMs) and agentic frameworks like [LangChain](https://langchain.com/) and [CrewAI](https://www.crewai.com/) into production. Spoiler alert: it's a wild ride, flaws and all!

Sure, these tools are reliable...ish. They're consistent...ish. But are they perfect? Not quite. Sometimes, even with simple text, different models can give you wildly different results 🔍.

Designing an LLM-based solution isn't just plug-and-play. You've got to juggle tokens, output formats (think JSON), and the cost and reliability of parameters like `temperature`. And don't even get me started on prompt engineering – it's an art form, not a science 😎!

Oh, and did I mention tools? LLMs can now use them, but in reality, creating custom tools can sometimes lead to a bigger codebase. Sometimes, a simple script does the trick better.

Now, let's talk unit testing. Setting up self-checking for LLMs can seriously boost response accuracy. But here's the catch: writing these self-checking methods is a whole other ball game because every agent and every API use case is different.... be prepared for a cost hike and a flurry of API calls 💸.

On the other hand, when you need to go through hundreds or thousands of paragraphs to extract valuable information, you need brains... and lots of them 🧠🧠🧠.

LLMs are the only solution for such tasks at scale!

<br>

### The Takeaway

So, what's the takeaway? The fewer decisions a model has to make, the more reliable it will be. But that also means more API calls and, yep, higher costs.

if you went that far, I hope you enjoyed this tech adventure🎉!

*Love,*
*Dimitris*

<br>

<br>

---

**Resources and Inspiration**

[JohannesJolkkonen: Knowledge Graph + Pythoon  GitHub)](https://github.com/JohannesJolkkonen/funktio-ai-samples/blob/main/knowledge-graph-demo/notebook.ipynb)

[Self-Reflective RAG with LangGraph (langchain.dev)](https://blog.langchain.dev/agentic-rag-with-langgraph/)

[Whats next for AI agentic workflows ft. Andrew Ng of AI Fund](https://www.youtube.com/watch?v=sal78ACtGTc)

[Going Meta - Ep 27: Building a Reflection Agent with LangGraph](https://www.youtube.com/watch?v=Sra-1xhNn28)

[Convert any Text Data into a Knowledge Graph (using LLAMA3 + GROQ)](https://www.youtube.com/watch?v=ky8LQE-82xs&ab_channel=GeraldusWilsen)

*Websraping alternative* [Scrapegraph.ai](https://github.com/VinciGit00/Scrapegraph-ai)
