import csv
import time
import json
import mariadb
import sys
import uuid
import time
import pytz
from datetime import datetime
from confluent_kafka import Producer
from fastapi import Depends, FastAPI, Header, Request, Body
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates

producer_conf = {
    'bootstrap.servers': 'kafka-cluster:9092',
    'batch.size': 1
}

producer = Producer(producer_conf)

keys = ["id", "timestamp", "username", "text"]

app = FastAPI()

templates = Jinja2Templates(directory="templates")

@app.get("/trending")
async def get_trending_topics(request: Request):
    trending_hashtags = fetch_trending_hashtags()
    return templates.TemplateResponse("trending.html", {"request": request, "results": trending_hashtags})

@app.post("/post")
def send_post(post_content: str = Body(..., embed=True)):
    json_object = {
        "id": str(uuid.uuid4()),
        "timestamp": current_datetime(),
        "username": "dummy_username",
        "text": post_content
    }
    json_object = json.dumps(json_object, indent = 4)
    producer.produce(topic="posts", value=json_object.encode('utf-8'))
    producer.flush()
    return {"message": "Received string: " + post_content}

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        return super().default(obj)

def fetch_trending_hashtags():
    try:
        conn = mariadb.connect(
            user="root",
            password="mysecretpw",
            host="my-app-mariadb-service",
            port=3306,
            database="user_posts"
        )

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trending_hashtags ORDER BY window_end DESC, counter DESC LIMIT 5")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        results = []
        for row in rows:
            result = dict(zip(columns, row))
            results.append(result)

        cursor.close()
        conn.close()

        return jsonable_encoder(results)

    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        sys.exit(1)

def current_datetime() -> str:
    utc_datetime = datetime.now(pytz.utc)
    cet_timezone = pytz.timezone('CET')
    cet_datetime = utc_datetime.astimezone(cet_timezone)
    formatted_datetime = cet_datetime.strftime("%a %b %d %H:%M:%S")
    current_year = datetime.now().year
    return f"{formatted_datetime} CET {current_year}"