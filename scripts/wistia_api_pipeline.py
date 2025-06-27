import json
import boto3
import pandas as pd
import requests
from datetime import datetime
import pyarrow.json as paj
import pyarrow as pa, pyarrow.parquet as pq
from datetime import datetime, timedelta
import re
from dateutil.relativedelta import relativedelta
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sys.argv += ["--JOB_NAME=local-test"]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# --- Config --
API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]
S3_BUCKET = "wistiaproject"
S3_PREFIX_MEDIA = "data/dim_media/"
S3_PREFIX_ENGAGEMENTS = "data/fact_engagements/"
S3_PREFIX_ENGAGEMENTS_BD = "data/fact_engagements_byday/"
S3_PREFIX_EVENTS = "data/fact_events/"
S3_PREFIX_GRAPH = "data/fact_engagement_graph/"
S3_PREFIX_VISITORS = "data/dim_visitors/"


# --- AWS Clients ---
s3 = boto3.client("s3")

# --- Function to fetch media dim ---
def fetch_media(media_id):
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# --- Function to fetch engagement metrics ---
def fetch_engagements(media_id):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# --- Function to fetch engagement by day metrics ---
def fetch_engagements_by_day(media_id, start_date, end_date):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json?start_date={start_date}&end_date={end_date}"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()    

# --- Function to fetch engagement graph data ---
def fetch_graph_data(media_id):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}/engagement.json"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# --- Function to fetch events ---
def fetch_events_page(media_id, start_date, end_date, page=1, per_page=100):
    url = f"https://api.wistia.com/v1/stats/events.json?media_id={media_id}&start_date={start_date}&end_date={end_date}&per_page={per_page}&page={page}"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    events = response.json()
    return events 

def fetch_events_all(media_id, start_date, end_date):
    page = 1
    all_events = []
    while True:
        evs = fetch_events_page(media_id, start_date, end_date, page)
        if not evs:
            break
        all_events.extend(evs)
        print(f"Fetched page {page}, {len(evs)} events")
        page += 1
    return all_events

def get_existing_months():
    paginator = s3.get_paginator("list_objects_v2")
    months = set()
    pattern = re.compile(r"fact_events_(\d{4}-\d{2})\.(?:csv|parquet)$")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX_EVENTS):
        for obj in page.get("Contents", []):
            key = obj["Key"].split("/")[-1]  # strip folders
            match = pattern.match(key)
            if match:
                months.add(match.group(1))

    return sorted(months)


def month_ranges(n_months=24):
    today = datetime.utcnow().replace(day=1)
    all_months = []
    curr = today - relativedelta(months=n_months-1)

    for _ in range(n_months):
        start = curr
        end = start + relativedelta(months=1) - timedelta(days=1)
        label = start.strftime("%Y-%m")
        all_months.append((start.date().isoformat(), end.date().isoformat(), label))
        curr += relativedelta(months=1)

    existing = get_existing_months()
    if not existing:
        return all_months

    last = existing[-1]
    idx = next((i for i, (_, _, label) in enumerate(all_months) if label == last), 0)
    return all_months[idx:]


# --- Main Logic ---
def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    
    job.init(args["JOB_NAME"], args)
    
    today = datetime.now().date()
    media = []
    engagements = []
    graph = []

    print("Start")
    for media_id in MEDIA_IDS:
        media_results = fetch_media(media_id)
        media.append(media_results)

        engagement_results = fetch_engagements(media_id)
        engagement_results = {'media_id': media_id, 'date': today, **engagement_results}
        engagements.append(engagement_results)

        
        graph_results = fetch_graph_data(media_id)
        graph_results = {'media_id': media_id, **graph_results}    
        graph.append(graph_results)


    for start, end, label in month_ranges():

        end_date = datetime.fromisoformat(end).date()
        if end_date > today:
            end_date = today
            end = end_date.isoformat()
        events = []
        visitors = []
        engagements_by_day = []
        for media_id in MEDIA_IDS: 
            eng_list = fetch_engagements_by_day(media_id, start, end)
            for entry in eng_list:
                entry['media_id'] = media_id
            
            engagements_by_day.append(eng_list)
            


            raw  = fetch_events_all(media_id, start, end)
            for ev in raw:
                ev.pop("conversion_data", None)
                lat = ev.get("lat", None)
                if lat == '' or (isinstance(lat, str) and lat.strip() == ''):
                    ev['lat'] = 0.0
                lon = ev.get("lon", None)
                if lon == '' or (isinstance(lon, str) and lon.strip() == ''):
                    ev['lon'] = 0.0
                visitor_id = ev.get("visitor_key", None)
                ip_address = ev.get("ip", None)
                country = ev.get("country", None)
                visitors.append({
                    "visitor_id": visitor_id,
                    "ip_address": ip_address,
                    "country": country
                })
                ev.pop("ip", None)
                ev.pop("country", None)
                events.append(ev)


        nested = engagements_by_day  # list of lists, each sublist per media_id
        flat = [e for media_list in nested for e in media_list]
        df = pd.DataFrame(flat)
        df = df.astype({
            'date': 'string',
            'load_count': 'int64',
            'play_count': 'int64',
            'hours_watched': 'float64',
            'media_id': 'string'
        })
        df = df[['media_id', 'date', 'load_count', 'play_count', 'hours_watched']]
        # engagements_bd_df = pd.DataFrame(engagements_by_day)
        key = f"{S3_PREFIX_ENGAGEMENTS_BD}fact_engagements_byday_{label}.parquet"
        df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
        s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
        print(f"✅ Uploaded engagments by day to s3://{S3_BUCKET}/{key}")
        

        if len(events) == 0:
            print(f"No events for {label}. Skipping.")
            continue  

        events_df = pd.DataFrame(events)
        key = f"{S3_PREFIX_EVENTS}fact_events_{label}.parquet"
        events_df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
        s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
        print(f"✅ Uploaded events to s3://{S3_BUCKET}/{key}")

        visitors_df = pd.DataFrame(visitors)
        key = f"{S3_PREFIX_VISITORS}fact_visitors_{label}.parquet"
        visitors_df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
        s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
        print(f"✅ Uploaded visitors to s3://{S3_BUCKET}/{key}")
    
    media_df = pd.DataFrame(media)
    key = f"{S3_PREFIX_MEDIA}dim_media.parquet"
    media_df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
    s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
    print(f"✅ Uploaded media details to s3://{S3_BUCKET}/{key}")

    engagements_df = pd.DataFrame(engagements)
    key = f"{S3_PREFIX_ENGAGEMENTS}fact_engagements_{today}.parquet"
    engagements_df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
    s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
    print(f"✅ Uploaded engagments to s3://{S3_BUCKET}/{key}")

    graph_df = pd.DataFrame(graph)
    key = f"{S3_PREFIX_GRAPH}fact_engagement_graph.parquet"
    graph_df.to_parquet(f"/tmp/{key.split('/')[-1]}", index=False)
    s3.upload_file(f"/tmp/{key.split('/')[-1]}", S3_BUCKET, key)
    print(f"✅ Uploaded graph data to s3://{S3_BUCKET}/{key}")
    
    job.commit()


main()