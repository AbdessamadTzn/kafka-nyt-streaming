import requests
import json
from kafka import KafkaProducer
import time
import os
from dotenv import load_dotenv

load_dotenv()
NYT_STREAMING_API = os.getenv('NYT_STREAMING_API')

print(f"API Key loaded: {'Yes' if NYT_STREAMING_API else 'No'}")

try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka producer connected!")
    
    response = requests.get(f'https://api.nytimes.com/svc/topstories/v2/home.json?api-key={NYT_STREAMING_API}')
    print(f"API Response status: {response.status_code}")
    
    if response.status_code == 200:
        articles = response.json().get("results", [])[:5]
        print(f"Found {len(articles)} articles")
        
        for article in articles:
            data = {
                'title': article['title'],
                'published_date': article['published_date'],
                'section': article['section']
            }
            producer.send('nyt-articles', data)
            print(f"Sent: {data['title']}")
            
        producer.flush() #Force sending
        print("All messages sent!")
    else:
        print(f"API Error: {response.text}")
        
except Exception as e:
    print(f"Error: {e}")