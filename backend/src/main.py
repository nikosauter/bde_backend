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
from fastapi import Depends, FastAPI, Header, Request, Body, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': 'kafka-cluster:9092',
    'batch.size': 1
}
producer = Producer(producer_conf)

# FastAPI configuration
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/templates", StaticFiles(directory="templates"), name="templates")

################################################################
### FastAPI endpoints for sending and retrieving information ###
################################################################
@app.get("/trending")
async def get_trending_topics(request: Request, selected_date: str = Query(default=None)):
    return get_hashtags_rendered(request, selected_date)

@app.get("/discover_trending")
async def get_trending_topics_from_day(selected_date: str = Query(default=None)):
    return get_hashtags_rendered(selected_date=selected_date)

@app.post("/post")
def send_post(post_content: str = Body(..., embed=True)):
    if post_content == "":
        return
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

# Get trending hashtags as rendered html or json
def get_hashtags_rendered(request: Request = None, selected_date: str = Query(default=None)):
    if selected_date:
        return fetch_trending_hashtags(date=datetime.strptime(selected_date, "%Y-%m-%d").date())
    else:
        trending_hashtags = fetch_trending_hashtags()
    return templates.TemplateResponse("trending.html", {"request": request, "results": trending_hashtags})

# Fetch trending hashtags from database
def fetch_trending_hashtags(date=None):
    conn = connect_to_db()
    cursor = conn.cursor()
    if date is not None:
        query = """
                    SELECT * FROM trending_hashtags
                    WHERE window_end <= %s + INTERVAL 1 DAY AND window_end >= %s - INTERVAL 3 DAY
                    ORDER BY window_end DESC, counter DESC LIMIT 5
                """
        cursor.execute(query, (date, date))
    else:
        cursor.execute("SELECT * FROM trending_hashtags "
                "WHERE window_end >= CURDATE() - INTERVAL 3 DAY "
                "ORDER BY window_end DESC, counter DESC LIMIT 5")

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in rows]
    cursor.close()
    conn.close()
    return jsonable_encoder(results)

def connect_to_db():
    try:
        conn = mariadb.connect(
            user="root",
            password="mysecretpw",
            host="my-app-mariadb-service",
            port=3306,
            database="user_posts"
        )
        return conn
    except mariadb.Error as e:
        sys.exit(1)

def current_datetime() -> str:
    utc_datetime = datetime.now(pytz.utc)
    cet_timezone = pytz.timezone('CET')
    cet_datetime = utc_datetime.astimezone(cet_timezone)
    formatted_datetime = cet_datetime.strftime("%a %b %d %H:%M:%S")
    current_year = datetime.now().year
    return f"{formatted_datetime} CET {current_year}"
