import requests
import re

OLLAMA_SERVER_URL = 'http://192.168.10.92:11434/api/generate'


def generate_name(column_data):
    """Generate text using the Ollama API."""
    prompt = (
    f"here is my first 4 rows from table: {column_data}. Based on the given columns, try to deduce the possible column names. for example if following data in column: ‘Иван’, ‘Андрей’, ‘Алексей’, then return ‘first_name’ column"
    f"Do not include descriptions or any additional text, only the column names."
    f"Choose one name."
    f"I need only one name."
    )
    payload = {
        "model" : 'llama3.1:70b',
        "prompt": prompt,
        "stream" : False
            }
    res = requests.post(OLLAMA_SERVER_URL, json=payload)
    if res.status_code == 200:
        str = res.json()['response']
        str = str.split('\n')
        return str[0]
    else:
        return res.raise_for_status()




