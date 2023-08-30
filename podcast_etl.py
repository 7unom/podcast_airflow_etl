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
     # This will tell airflow to create database before running `extract_episodes()`
    

    # Task function to get podcast episodes from the feed
    @task()
    def extract_episodes():
        """
    Get the latest podcast episodes from the Marketplace podcast feed.

    Returns:
        A list of dictionaries containing the podcast episodes.
    """
        data =requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes =feed["rss"]["channel"]["item"] # Get the list of episodes
        # Print the number of episodes found
        print(f"Found {len(episodes)} episodes")
        return episodes

    # Fetch episodes from the feed
    podcast_episodes = extract_episodes()

    # Set task dependencies: create_database -> extract_episodes
    create_database.set_downstream(podcast_episodes) # This will notify airflow to create database before running `extract_episodes()`


    # Task function to transfrom extracted episodes to meet requirements
    @task
    def transform_episodes(episodes):
        """
    Transforms the extracted podcast episodes to meet the requirements of the project.

    Args:
        episodes: A list of dictionaries containing the extracted podcast episodes.
    
    Returns:A list of dictionaries containing the transformed podcast episodes.
    """
        transformed_episodes = []

        # Iterate over the extracted episodes
        for episode in episodes:
            # Get the link, title, published date, and description of the episode
            link = episode.get('link')
            title = episode.get('title')
            published = episode.get('pubDate')
            description = episode.get('description')
        
            # Convert the published date to a more readable format
            pub_object = datetime.strptime(published[:-6], "%a, %d %b %Y %H:%M:%S")
            pubDate = pub_object.strftime( "%Y-%m-%d")
            pubTime = pub_object.strftime("%H:%M:%S")

            # Clean the description by removing the HTML tags
            cleaned_description = description.replace('<p>', '').replace('</p>', '').strip()
            description_string = ' '.join(cleaned_description)
            return link, title, pubDate, pubTime, cleaned_description
        
        # Create a dictionary of the transformed episode data
        transformed_episodes.append({
            "link": link,
            'title': title,
            'pubDate': pubDate,
            'pubTime': pubTime,
            'description': description_string
        })
        return transformed_episodes
    
    transformed_podcast = transform_episodes(podcast_episodes)


    # Task function to load new episodes into the SQLite database
    @task()
    def load_episodes(transformed_episodes):
        """
    Load the newly extracted podcast episodes into the SQLite database.

    Args:
        transformed_episodes: A list of dictionaries containing the transformed podcast episodes.
    """
        # Get the database connection hook
        hook = SqliteHook(sqlite_conn_id='podcasts')

        # Get the list of stored episodes
        stored_episodes = hook.get_pandas_df('SELECT * FROM episodes')
        
         # Create a list of new episodes
        new_episodes = []
        for episode in transformed_episodes:
            link = episode[0]
            if link not in stored_episodes['link'].values:
                filename = f"{link.split('/')[-1]}.mp3"
                new_episodes.append([link, episode[1], episode[2], episode[3], episode[4], filename])
        # Insert the new episodes into the database
        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=['link','title', 'pubDate', 'pubTime','description', 'filename'])

    # Load new episodes into the database
    load_episodes(transformed_podcast)

    # Task function to download podcast episodes
    @task()
    def download_episodes(episodes):
        """
    Download the podcast episodes to the local filesystem.

    Args:
        episodes: A list of dictionaries containing the podcast episodes.
    """
        # Iterate over the episodes
        for episode in episodes:
            # Get filename
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            # Get the audio path
            audio_path = os.path.join('episodes', filename)

            # Check if the file already exists
            if not os.path.exists(audio_path):
                # Download the file
                print(f"Downloading {filename}")
                audio = requests.get(episodes['enclosure']['url'])
                with open(audio_path, 'wb+') as f:
                    f.write(audio.content)
                    
     # Download podcast episodes
    download_episodes(podcast_episodes)

# Create the DAG object
summary = podcast_summary()
