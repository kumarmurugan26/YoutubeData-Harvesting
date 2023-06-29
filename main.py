import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import psycopg2
from datetime import datetime
from googleapiclient.errors import HttpError

cnx = psycopg2.connect(host="localhost",
                       user="postgres",
                       password="9842",
                       port=5432,
                       database="postgres1")
cursor = cnx.cursor()

conn = pymongo.MongoClient("mongodb://kumarmurugan9:kumarmurugan@ac-jkm7ml8-shard-00-00.vbzfdyw.mongodb.net:27017,ac-jkm7ml8-shard-00-01.vbzfdyw.mongodb.net:27017,ac-jkm7ml8-shard-00-02.vbzfdyw.mongodb.net:27017/?ssl=true&replicaSet=atlas-jfe7tk-shard-0&authSource=admin&retryWrites=true&w=majority")
db = conn['youtube_data']
collection = db['channels']
api_key = 'AIzaSyAlf_EjyEDvMsm0s0JQGCVKODKSi9YNgiw'
youtube = build("youtube", "v3", developerKey=api_key)
def main():
    st.title("YouTube Data Harvesting")
    st.write("---")

    menu = ["Retrieve YouTube Channel Data", "Migrate Data to SQL", "Search in SQL Database"]
    choice = st.sidebar.selectbox("Select an option", menu)

    if choice == "Retrieve YouTube Channel Data":
        store_data_in_mongodb()
    elif choice == "Migrate Data to SQL":
        migrate_data_to_sql()
    elif choice == "Search in SQL Database":
        search_sql_database()


def get_channel_data(channel_id):
    response = youtube.channels().list(
        part="snippet, statistics, contentDetails",
        id=channel_id
    ).execute()

    channel_data = {}
    if response.get("items"):
        channel = response["items"][0]
        channel_data["Channel id"] = channel_id
        channel_data["Channel Name"] = channel["snippet"]["title"]
        channel_data["Subscribers"] = channel["statistics"]["subscriberCount"]
        channel_data["Total Video Count"] = channel["statistics"]["videoCount"]
        channel_data["channel_type"] = channel["snippet"].get("channelType","Unknown")
        channel_data["channel_description"] = channel["snippet"]["description"]
        channel_status = channel.get("status", {}).get("privacyStatus", "Unknown")
        channel_data["channel_status"] = channel_status

        channel_data["Playlist"] = get_playlist_data(channel_id)
    return channel_data

def get_playlist_data(channel_id):
    playlist = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId="UCduIoIMfD8tT3KoU0-zBRgQ",
            maxResults=50,
            pageToken=next_page_token
        )

        response = request.execute()

        for i in response['items']:
            playlist_details = {'Playlist ID': i['id'],
                                'channel_id': i['snippet']['channelId'],
                                'Playlist Title': i['snippet']['title']}

            playlist.append(playlist_details)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return playlist

def get_video_ids(playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        playlist_items = response['items']

        for playlist_item in playlist_items:
            video_id = playlist_item['contentDetails']['videoId']
            video_ids.append(video_id)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return video_ids

# print(len(video_ids))

def get_video_details1(video_ids):
    video_data = []
    next_page_token = None
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response["items"]:
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})

            duration = content_details.get("duration", "")
            duration = duration[2:]  # Remove "PT" from the beginning

            hours = 0
            minutes = 0
            seconds = 0

            if 'H' in duration:
                hours_index = duration.index('H')
                hours = int(duration[:hours_index])
                duration = duration[hours_index + 1:]

            if 'M' in duration:
                minutes_index = duration.index('M')
                minutes = int(duration[:minutes_index])
                duration = duration[minutes_index + 1:]

            if 'S' in duration:
                seconds_index = duration.index('S')
                seconds = int(duration[:seconds_index])

            duration_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            video_info = {
                "video_id": video["id"],
                "title": snippet["title"],
                "description": snippet["description"],
                "tags": snippet.get("tags", []),
                "publishedAt": snippet["publishedAt"],
                "thumbnail_url": snippet["thumbnails"]["default"]["url"],
                "viewCount": statistics.get("viewCount", 0),
                "likeCount": statistics.get("likeCount", 0),
                "favoriteCount": statistics.get("favoriteCount", 0),
                "commentCount": statistics.get("commentCount", 0),
                "duration": duration_formatted,
                "definition": content_details.get("definition", ""),
                "caption": content_details.get("caption", ""),
                "Comments": get_comment_data(video_id)
            }
            video_data.append(video_info)
        return video_data

def get_comment_data(video_ids):
    comments_data = []
    for ids in video_ids:
        try:
            video_data_request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50
            ).execute()
            video_info = video_data_request['items']
            for comment in video_info:
                comment_info = {
                    'Video_id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                }
                comments_data.append(comment_info)
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                comment_info = {
                    'Video_id': ids,
                    'Comment_Id': 'comments_disabled',
                }
                comments_data.append(comment_info)
            else:
                print(f"An error occurred while retrieving comments for video: {ids}")
                print(f"Error details: {e}")
    return comments_data

def store_data_in_mongodb():
    st.header("Store Data in MongoDB")
    channel_id = st.text_input("Enter the YouTube channel ID")

    if st.button("Store Data"):
        channel_data = get_channel_data(channel_id)
        # Insert channel data into the collection
        collection.insert_one(channel_data)
        st.write("Channel Name :",channel_data["Channel Name"])
        st.write("Subscribers :",channel_data["Subscribers"])
        st.write("Total Video Count :", channel_data["Total Video Count"])

def migrate_data_to_sql():

    st.header("Migrate Data to SQL")
    channel_names = [channel["Channel Name"] for channel in collection.find({}, {"Channel Name": 1})]


    # Create a dropdown to select a channel
    selected_channel = st.selectbox("Select a channel", channel_names)

    if st.button("Migrate Data"):

        channel_data = collection.find_one({"Channel Name": selected_channel})
        if channel_data:
            create_sql_tables()
            insert_data_to_sql(channel_data)
            st.write("Data migrated to SQL successfully.")
        else:
            st.write("Channel data not found in MongoDB.")

def create_sql_tables():
    # Create channel table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel (
            channel_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            subscribers INT,
            video_count INT
        )
    """)

    # Create playlist table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS playlist (
            channel_id VARCHAR(255),
            playlist_id VARCHAR(255) PRIMARY KEY,
            playlist_name VARCHAR(255),
            FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
        )
    """)

    # Create video table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video (
            id SERIAL PRIMARY KEY,
            playlist_id VARCHAR(255),
            video_id VARCHAR(255),
            title VARCHAR(255),
            description TEXT,
            published_date VARCHAR(255),
            views INT,
            likes INT,
            comment_count INT,
            duration INT,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255),
            FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
        )
    """)

    # Create comment table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment (
            id SERIAL PRIMARY KEY,
            video_id INT,
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_published_date VARCHAR(255),
            FOREIGN KEY (video_id) REFERENCES video(id)
        )
    """)

    cnx.commit()

def insert_data_to_sql(channel_data):
    channel_values = (channel_data["Channel id"],channel_data["Channel Name"], channel_data["Subscribers"], channel_data["Total Video Count"])
    cursor.execute("""
        INSERT INTO channel (channel_id, name, subscribers, video_count)
        VALUES (%s,%s, %s, %s)
         """, channel_values)
    # channel_id = cursor.fetchone()[0]
    playlist = get_playlist_data(channel_id)

    for playlist_details in playlist:
            playlist_values = (playlist_details['channel_id'], playlist_details['Playlist ID'], playlist_details['Playlist Title'])
            cursor.execute("""
                INSERT INTO playlist (channel_id, playlist_id, playlist_name)
                VALUES (%s, %s, %s)
                RETURNING id
            """, playlist_values)
            playlist_id = cursor.fetchone()[0]

    for video_data in playlist_data["Videos"]:
        video_values = (
            playlist_id,
            video_data["video id"],
            video_data["title"],
            video_data["description"],
            video_data["publishedAt"],
            video_data["viewCount"],
            video_data["likeCount"],
            video_data["commentCount"],
            video_data["duration"],
            video_data["thumbnail_url"],
            video_data["caption"])

        cursor.execute("""
            INSERT INTO video (
                playlist_id,
                video_id,
                title,
                description,
                published_date,
                views,
                likes,
                comment_count,
                duration,
                thumbnail,
                caption_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            RETURNING id
             """, video_values)

        video_id = cursor.fetchone()[0]
        for comment in comments:
            comment_values = (
                video_id,
                comment["comment_text"],
                comment["comment_author"],
                comment["comment_published_date"]
            )
            cursor.execute("""
                       INSERT INTO comment (video_id, comment_text, comment_author, comment_published_date)
                       VALUES (%s, %s, %s, %s)
                   """, comment_values)

        cnx.commit()

def search_sql_database():
    st.header("Search in SQL Database")
    data = [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ]

    def execute_function(option):
        if option == data[0]:

            query = """
        SELECT v.title, c.name
        FROM video v,channel c,playlist p
        where c.id = p.channel_id and p.id = v.playlist_id
    """
            cursor.execute(query)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=['Video Name', 'Channel Name'])

        def execute_function(option):
            if option == data[0]:
                query = """
                    SELECT v.title, c.name
                    FROM video v
                    JOIN playlist p ON v.playlist_id = p.id
                    JOIN channel c ON c.id = p.channel_id
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Channel Name'])
                st.table(df)

            elif option == data[1]:
                query = """
                    SELECT name, video_count
                    FROM channel
                    ORDER BY video_count DESC
                    LIMIT 1
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Video Count'])
                st.table(df)

            elif option == data[2]:
                query = """
                    SELECT v.title, c.name
                    FROM video v
                    JOIN playlist p ON v.playlist_id = p.id
                    JOIN channel c ON c.id = p.channel_id
                    ORDER BY v.duration DESC
                    LIMIT 10
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Channel name'])
                st.table(df)

            elif option == data[3]:
                query = """
                    SELECT v.title, v.comment_count
                    FROM video v
                    JOIN playlist p ON v.playlist_id = p.id
                    JOIN channel c ON c.id = p.channel_id
                    ORDER BY v.comment_count DESC
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Comment Count'])
                st.table(df)

            elif option == data[4]:
                query = """
                    SELECT v.title, v.likes, c.name
                    FROM video v
                    JOIN playlist p ON v.playlist_id = p.id
                    JOIN channel c ON c.id = p.channel_id
                    ORDER BY v.likes DESC
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Video Likes', 'Channel name'])
                st.table(df)

            elif option == data[5]:
                query = """
                    SELECT v.title, v.likes, v.dislikes
                    FROM video v
                    JOIN playlist p ON v.playlist_id = p.id
                    JOIN channel c ON c.id = p.channel_id
                    ORDER BY v.likes DESC
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Video Likes', 'Video Dislikes'])
                st.table(df)

            elif option == data[6]:
                query = """
                    SELECT c.name, SUM(v.views) AS total_views
                    FROM channel c
                    JOIN playlist p ON c.id = p.channel_id
                    JOIN video v ON p.id = v.playlist_id
                    GROUP BY c.name
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Total Views'])
                st.table(df)

            elif option == data[7]:
                query = """
                    SELECT c.name, v.published_date
                    FROM channel c
                    JOIN playlist p ON c.id = p.channel_id
                    JOIN video v ON p.id = v.playlist_id
                    WHERE EXTRACT(YEAR FROM v.published_date) = 2022
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Published Date'])
                st.table(df)

            elif option == data[8]:
                query = """
                    SELECT c.name, AVG(v.duration) AS avg_duration
                    FROM channel c
                    JOIN playlist p ON c.id = p.channel_id
                    JOIN video v ON p.id = v.playlist_id
                    GROUP BY c.name
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Avg Duration'])
                st.table(df)

            elif option == data[9]:
                query = """
                    SELECT v.title, v.comment_count, c.name
                    FROM channel c
                    JOIN playlist p ON c.id = p.channel_id
                    JOIN video v ON p.id = v.playlist_id
                    WHERE v.comment_count = (
                        SELECT MAX(comment_count)
                        FROM video
                        WHERE playlist_id = v.playlist_id
                    )
                """
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Name', 'Video count', 'Channel Name'])
                st.table(df)

            selected_option = st.selectbox("Select a search option", data)
            # Execute function based on selected option
            if st.button("Execute"):
                execute_function(selected_option)
if __name__ == "__main__":
    main()















