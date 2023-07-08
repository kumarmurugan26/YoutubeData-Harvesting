import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import numpy as np
from pymongo import MongoClient
import psycopg2
from datetime import datetime
from googleapiclient.errors import HttpError

pg_connection = psycopg2.connect(
                                host="localhost",
                                database="postgres1",
                                user="postgres",
                                password="9842"
                                )
pg_cursor = pg_connection.cursor()

mongo_client = MongoClient("mongodb://kumarmurugan9:kumarmurugan@ac-jkm7ml8-shard-00-00.vbzfdyw.mongodb.net:27017,ac-jkm7ml8-shard-00-01.vbzfdyw.mongodb.net:27017,ac-jkm7ml8-shard-00-02.vbzfdyw.mongodb.net:27017/?ssl=true&replicaSet=atlas-jfe7tk-shard-0&authSource=admin&retryWrites=true&w=majority")
db = mongo_client['youtube_data']
collection1 = db['channel_details']
collection2 = db['video_details']
collection3 = db['comments_details']

api_key = 'AIzaSyAlf_EjyEDvMsm0s0JQGCVKODKSi9YNgiw'
youtube = build("youtube", "v3", developerKey=api_key)


st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
    page_icon="▶️",
    layout="wide"
)
menu = ["Extract", "Migrate", "View"]
selected = st.sidebar.selectbox("Select an option", menu)
icons=["house", "gear", "tools"],
styles={"nav-link": {"font": "sans serif", "font-size": "20px", "text-align": "centre"},
                               "nav-link-selected": {"font": "sans serif", "background-color": "#088F8F"},
                               "icon": {"font-size": "20px"}
                               }


def get_channel_data(channel_ids):
    channel_data = []
    request = youtube.channels().list(
        id=channel_ids,
        part='snippet,statistics,contentDetails'
    )

    response = request.execute()

    for i in response['items']:
        data = {"Channel_Name": i['snippet']['title'],
                "Channel_Id": i['id'],
                "Subscription_Count": i['statistics']['subscriberCount'],
                "Channel_Views": i['statistics']['viewCount'],
                "Channel_Description": i['snippet']['description'],
                "Total_video_count": i['statistics']['videoCount'],
                "Playlist_Id": i['contentDetails']['relatedPlaylists']['uploads'],
                }

        channel_data.append(data)

    return (channel_data)



def get_video_ids(channel_ids):
    video_ids = []

    for channel_id in channel_ids:
        next_page_token = None

        request = youtube.channels().list(
            id=channel_id,
            part='snippet,statistics,contentDetails'
        )
        response = request.execute()

        if 'items' not in response:
            print(f"No items found for channel ID: {channel_id}")
            continue

        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

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

def get_video_details1(video_ids):
    video_data = []
    next_page_token = None
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50]),
            pageToken=next_page_token
        )
        response = request.execute()

        for video in response["items"]:
            published_date_str = video["snippet"]["publishedAt"]
            published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

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
                "Channel_name": video['snippet']['channelTitle'],
                "Channel_id": video['snippet']['channelId'],
                "Video_id": video["id"],
                "title": snippet["title"],
                "description": snippet["description"],
                "tags": snippet.get("tags", []),
                "publishedAt": formatted_published_date,
                "thumbnail_url": snippet["thumbnails"]["default"]["url"],
                "viewCount": statistics.get("viewCount", 0),
                "likeCount": statistics.get("likeCount", 0),
                "favoriteCount": statistics.get("favoriteCount", 0),
                "commentCount": statistics.get("commentCount", 0),
                "duration": duration_formatted,
                "definition": content_details.get("definition", ""),
                "caption": content_details.get("caption", "")
            }
            video_data.append(video_info)
        next_page_token = response.get("nextPageToken")
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
                published_date_str = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
                formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

                comment_info = {
                    'Video_id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published_At': formatted_published_date
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



if selected == "Extract":
    st.write("### Enter a YouTube Channel id -")
    channel_ids = st.text_input("Channel id").split(',')
    if channel_ids and st.button("Extract Data from API"):
        channel_details = get_channel_data(channel_ids)
        st.write(f'### Channel Data Extracted Successfully')
        st.write(channel_details)
    if st.button("Upload Data to MongoDB Atlas"):
        with st.spinner('Uploading....'):
            channel_details = get_channel_data(channel_ids)
            videos_ids = get_video_ids(channel_ids)
            video_details = get_video_details1(videos_ids)
            comment_details = get_comment_data(videos_ids)

            collection1.insert_many(channel_details)
            collection2.insert_many(video_details)
            collection3.insert_many(comment_details)
            st.success("Data Uploaded Successfully")

if selected == "Migrate":
    st.write("### Data Migration from MongoDB Atlas to MySQL")

    def youtube_channel_names():
        channelname = []
        for i in db.channel_details.find():
            channelname.append(i['Channel_Name'])
        return channelname

    ch_names = youtube_channel_names()
    user_inp = st.selectbox("Select the channel for data migration:", options=ch_names)

    def create_sqltables():
        pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                Channel_Name VARCHAR(255),
                Channel_Id VARCHAR(255),
                Subscription_Count INT,
                Channel_Views INT,
                Channel_Description TEXT,
                Total_Video_Count INT,
                Playlist_Id VARCHAR(255)
            )
        """)

        pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                Channel_name VARCHAR(255),
                Channel_id VARCHAR(255),
                video_id VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                tags TEXT[],
                publishedAt TIMESTAMP,
                thumbnail_url VARCHAR(255),
                viewCount INTEGER,
                likeCount INTEGER,
                favoriteCount INTEGER,
                commentCount INTEGER,
                duration INTERVAL,
                definition VARCHAR(20),
                caption VARCHAR(20)
            )
        """)

        pg_cursor.execute("""
                       CREATE TABLE IF NOT EXISTS comments (
                           Video_Id VARCHAR(255),
                           Comment_Id VARCHAR(255),
                           Comment_Text TEXT,
                           Comment_Author VARCHAR(255),
                           Comment_Published_At TIMESTAMP
                       )
                   """)
        pg_connection.commit()
    def migrate_data():

        channel_details = collection1.find({"Channel_Name": "user_inp"}, {'_id': 0})
        for item in channel_details:
            values = (
                item['Channel_Name'],
                item['Channel_Id'],
                item['Subscription_Count'],
                item['Channel_Views'],
                item['Channel_Description'],
                item['Total_video_count'],
                item['Playlist_Id']
            )
            pg_cursor.execute("INSERT INTO channels VALUES (%s, %s, %s, %s, %s, %s, %s)", values)
        pg_connection.commit()

        for video_item in collection2.find({"Channel_name": "user_inp"}, {'_id': 0}):
            values = (
                video_item['Channel_name'],
                video_item['Channel_id'],
                video_item['Video_id'],
                video_item['title'],
                video_item['description'],
                video_item['tags'],
                video_item['publishedAt'],
                video_item['thumbnail_url'],
                video_item['viewCount'],
                video_item['likeCount'],
                video_item['favoriteCount'],
                video_item['commentCount'],
                video_item['duration'],
                video_item['definition'],
                video_item['caption']
            )
            pg_cursor.execute("INSERT INTO videos VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                              values)

        pg_connection.commit()

        for video_item in collection2.find({"Channel_name": "user_inp"}, {'_id': 0}):
            video_id = video_item['Video_id']
            for comment_item in collection3.find({"Video_id": video_id}, {'_id': 0}):
                comment_values = (
                    comment_item['Video_id'],
                    comment_item['Comment_Id'],
                    comment_item['Comment_Text'],
                    comment_item['Comment_Author'],
                    comment_item['Comment_Published_At']
                )
                pg_cursor.execute("INSERT INTO comments VALUES (%s, %s, %s, %s, %s)", comment_values)

        pg_connection.commit()


    if st.button("Migrate Data to MySQL"):
        with st.spinner('Migrating....'):
            create_sqltables()
            migrate_data()
            st.success("Data Migration Successful")



if selected == "View":
    st.write("## :yellow[Select a question to get the Query Results]")
    questions = st.selectbox('Questions',
                             ['- What are the names of all the videos and their corresponding channels?',
                              '- Which channels have the most number of videos, and how many videos do they have?',
                              '- What are the top 10 most viewed videos and their respective channels?',
                              '- How many comments were made on each video, and what are their corresponding video names?',
                              '- Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '- What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '- What is the total number of views for each channel, and what are their corresponding channel names?',
                              '- What are the names of all the channels that have published videos in the year 2022?',
                              '- What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '- Which videos have the highest number of comments, and what are their corresponding channel names?',])

    if questions == '- What are the names of all the videos and their corresponding channels?':
        pg_cursor.execute("""SELECT Title AS Video_name, Channel_name FROM videos ORDER BY Channel_name""")
        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- Which channels have the most number of videos, and how many videos do they have?':
        pg_cursor.execute("""SELECT Channel_name, Total_video_count FROM channels ORDER BY Total_video_count DESC""")
        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- What are the top 10 most viewed videos and their respective channels?':
        pg_cursor.execute("""SELECT Title AS title, viewcount, channel_name FROM videos
                            ORDER BY viewcount DESC LIMIT 10""")
        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- How many comments were made on each video, and what are their corresponding video names?':
        pg_cursor.execute("""SELECT commentcount, title AS Video_name
                            FROM videos""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- Which videos have the highest number of likes, and what are their corresponding channel names?':
        pg_cursor.execute("""SELECT title AS Video_name, likecount, channel_name FROM videos
                            ORDER BY likecount DESC""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        pg_cursor.execute("""SELECT likecount, title AS Video_name FROM videos""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- What is the total number of views for each channel, and what are their corresponding channel names?':
        pg_cursor.execute("""SELECT channel_views, channel_name FROM channels""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- What are the names of all the channels that have published videos in the year 2022?':
        pg_cursor.execute("""SELECT channel_name FROM videos
                             WHERE EXTRACT(YEAR FROM publishedat) = 2022
                             GROUP BY channel_name""")
        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)

    elif questions == '- What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        pg_cursor.execute("""SELECT channel_name, AVG(duration) AS average_duration 
                            FROM videos
                            GROUP BY channel_name""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        query_result = [(channel_name, str(average_duration)) for channel_name, average_duration in query_result]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)




    elif questions == '- Which videos have the highest number of comments, and what are their corresponding channel names?':
        pg_cursor.execute("""SELECT title AS Video_name, commentCount, channel_name FROM videos
                            ORDER BY commentCount DESC""")

        query_result = pg_cursor.fetchall()

        column_names = [desc.name for desc in pg_cursor.description]

        df = pd.DataFrame(query_result, columns=column_names, index=np.arange(1, len(query_result) + 1))
        st.write(df)



