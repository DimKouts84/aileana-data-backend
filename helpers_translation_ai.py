import json
# import requests
import time, datetime
# from openai import OpenAI
import os 
from dotenv import load_dotenv
from groq import Groq

load_dotenv()
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

# Check if the text is in Greek
def is_greek(text):
    # Simple heuristic to detect Greek text
    return any(ord(c) >= 913 and ord(c) <= 969 for c in text)


# Translate the Title of a Job listing
def translate_job_listings(job_listings):
    # Openllm = OpenAI(base_url=OPENAI_API_BASE, api_key=OPENAI_API_KEY)
    Groqllm = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    translated_list = []
    
    time.sleep(1)
    for job_listing in job_listings:

        title = job_listing["Job Listing Title"]
        
        if is_greek(title):
            time.sleep(2)
            
            # Send API Request for translation
            chat_completion = Groqllm.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": f'''translate '{title}' from Greek to English. 
                        Examples: 
                        if the user provides the words 'Εργάτης/τρια' then your output will be 'Worker'.
                        if the user provides the words 'Καθηγητής/τρια Χημείας' then your output will be 'Chemistry Professor'.
                        Provide ONLY the translated words(s) and NOTHING elese.
                        ''',
                    }
                ],
                model="llama-3.1-70b-versatile",
                # models= "llama-3.1-70b-versatile","llama3-70b-8192" or "mixtral-8x7b-32768"
                temperature=0,
            )
            translated_title = str(chat_completion.choices[0].message.content)
            
        else:
            translated_title = title
        job_listing["Job Listing Title"] = translated_title
        print(translated_title)
        translated_list.append(job_listing)
        
        # Write the list of job listings to a JSON file
        with open(f'{datetime.date.today()}_job_listings_translated.json', 'w', encoding='utf-8') as f:
            json.dump(translated_list, f, ensure_ascii=False )
    return translated_list


# Translate the description of the Job listing
def translate_job_description(job_description):
    # Openllm = OpenAI(base_url=OPENAI_API_BASE, api_key=OPENAI_API_KEY)
    Groqllm = Groq(api_key=os.environ.get("GROQ_API_KEY"))

    # Check if the text is in Greek
    if is_greek(job_description):
        # wait and then send API Request for translation
        time.sleep(4)
        chat_completion = Groqllm.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": f'''Translate '{job_description}' from Greek to English. 
                    Steps:
                    1) read the provided text in Greek.
                    2) Translate the text into English.
                    3) Provide the translation of the text.
                    
                    Example:
                    If the user provides the text 'Ζητείται υπάλληλος γραφείου για μερική απασχόληση.' then your output will be 'Office worker wanted for part-time employment.'
                    
                    Think step,by step and provide ONLY the translated textt and NOTHING else.
                    Do not include phrases like `here is the translation of the text:` or anythin simillar, provide ONLY the translated text.
                    ''',
                }
            ],
            model="llama3-70b-8192",
            # available models for GROQ => "llama3-70b-8192" or "mixtral-8x7b-32768"
            temperature=0,
        )
        translated_jobDescription = str(chat_completion.choices[0].message.content)
    else: #If not in Greek
        translated_jobDescription = job_description
    print(translated_jobDescription)
    return translated_jobDescription

'''
# Example usage of the job title translation function.
with open('job_listings.json', encoding='utf-8') as f:
    job_listings = json.load(f)
    

translated_list = translate_job_listings(job_listings)
# print(translated_list)

# Write the list of job listings to a JSON file
with open('job_listings-Translated.json', 'w', encoding='utf-8') as f:
    json.dump(job_listings, f)
'''

# Test the description translation function
# job_description = 'Ζητείται υπάλληλος γραφείου για μερική απασχόληση.'
# translate_job_description(job_description)
