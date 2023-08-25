from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator # Task to execute SQL commands in SQLite database
from airflow.providers.sqlite.hooks.sqlite import SqliteHook # Hook to interact with SQLite database
import pendulum
import requests
import xmltodict
import os


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
        published  TEXT,
        description TEXT,
        filename TEXT
        )""",
        
        sqlite_conn_id='podcasts' # tells airflow which connection to use to run the code
    )
    

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

    # Task function to load new episodes into the SQLite database
    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        stored_episodes = hook.get_pandas_df('SELECT * FROM episodes')
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_episodes["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=['link','title', 'published', 'description', 'filename'])

    # Load new episodes into the database
    load_episodes(podcast_episodes)

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
