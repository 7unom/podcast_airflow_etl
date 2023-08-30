from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator # Task to execute SQL commands in SQLite database
from airflow.providers.sqlite.hooks.sqlite import SqliteHook # Hook to interact with SQLite database
import pendulum
import requests
import xmltodict
import os
from datetime import datetime

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"


# Define the DAG with appropriate metadata
@dag(
    dag_id="podcast_summary",
    schedule_interval= '@daily',
    start_date=pendulum.datetime(2023,8,25),
    catchup=False, 
    )

def podcast_summary():

    # Task: Create a SQLite database table if it doesn't exist
    create_database = SqliteOperator(
        task_id ="create_table_sqlite",
        sql = r"""
        CREATE TABLE IF NOT EXISTS episodes(
        link TEXT PRIMARY KEY,
        title TEXT,
        pubDate  DATE,
        pubTime TIME,
        description TEXT,
        filename TEXT)
        """,
        
        sqlite_conn_id='podcasts' # tells airflow which connection to use to run the code
    )
     # This will tell airflow to create database before running `get_episodes()`
    

    # Task function to get podcast episodes from the feed
    @task()
    def get_episodes():
        data =requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes =feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes")
        return episodes

    # Fetch episodes from the feed
    podcast_episodes = get_episodes()

    # Set task dependencies: create_database -> get_episodes
    create_database.set_downstream(podcast_episodes) # This will tell airflow to create database before running `get_episodes()`

    @task
    def transform(episodes):
        transformed_episodes = []
        for episode in episodes:
            link = episode.get('link')
            title = episode.get('title')
            published = episode.get('pubDate')
            description = episode.get('description')
        
            pub_object = datetime.strptime(published[:-6], "%a, %d %b %Y %H:%M:%S")
            pubDate = pub_object.strftime( "%Y-%m-%d")
            pubTime = pub_object.strftime("%H:%M:%S")
            cleaned_description = description.replace('<p>', '').replace('</p>', '').strip()
            description_string = ' '.join(cleaned_description)
            return link, title, pubDate, pubTime, cleaned_description
        transformed_episodes.append({
            "link": link,
            'title': title,
            'pubDate': pubDate,
            'pubTime': pubTime,
            'description': description_string
        })
        return transformed_episodes
    
    transformed_podcast = transform(podcast_episodes)


    # Task function to load new episodes into the SQLite database
    @task()
    def load_episodes(transformed_episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        stored_episodes = hook.get_pandas_df('SELECT * FROM episodes')
        new_episodes = []
        for episode in transformed_episodes:
            link = episode[0]
            if link not in stored_episodes['link'].values:
                filename = f"{link.split('/')[-1]}.mp3"
                new_episodes.append([link, episode[1], episode[2], episode[3], episode[4], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=['link','title', 'pubDate', 'pubTime','description', 'filename'])

    # Load new episodes into the database
    load_episodes(transformed_podcast)

    # Task function to download podcast episodes
    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join('episodes', filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episodes['enclosure']['url'])
                with open(audio_path, 'wb+') as f:
                    f.write(audio.content)
     # Download podcast episodes
    download_episodes(podcast_episodes)

# Create the DAG object
summary = podcast_summary()
