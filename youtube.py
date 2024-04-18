from googleapiclient.discovery import build
import pandas as pd
from pymongo import MongoClient
import mysql.connector
import streamlit as st

def api_connect():
    api_id = "AIzaSyCcrU1hXmtG-siHxSVNrEoR-dvPgKbg0qw"
    api_service = "Youtube"
    api_version = "V3"

    youtube = build(api_service, api_version, developerKey = api_id)
    return youtube

youtube = api_connect()


def get_channel_info(channel_id):
    request = youtube.channels().list(
        part = "snippet, contentDetails, statistics",
        id = channel_id)
    
    response = request.execute()
    
    for i in response['items']:
        data = dict(channel_name = i['snippet']['title'],
                    channel_id = i['id'],
                    subscribers = i['statistics']['subscriberCount'],
                    views = i['statistics']['viewCount'],
                    total_videos = i['statistics']['videoCount'],
                    channel_description = i['snippet']['description'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads']                    
                   )
    
    return data
    


def video_id(channel_id):
    video_ids = []
    next_page_token = None
    
    while True:
        request = youtube.channels().list(
    part = "contentDetails",
    id = channel_id
    )
    
        response = request.execute()
        
        request1 = youtube.playlistItems().list(
                                    part = 'snippet',
                                    playlistId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                                    maxResults = 50,
                                    pageToken = next_page_token
                                    )

        response1 = request1.execute()


        for i in response1['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
        
    return video_ids


def get_video_information(video_ids):
    video_data = []
    
    for i in video_ids:
        request = youtube.videos().list(
        part = "snippet,contentDetails,statistics,topicDetails",
        id = i)
        
        response = request.execute()
        
        for item in response['items']:
            data = dict(
            video_id = item['id'],
            channel_id=item['snippet']['channelId'],
            video_name = item['snippet']['title'],
            video_description = item['snippet'].get('description'),
            published_date = item['snippet']['publishedAt'],
            view_count = item['statistics']['viewCount'],
            like_count = item['statistics'].get('likeCount'),
            favourite_count = item['statistics']['favoriteCount'],
            comment_count = item['statistics'].get('commentCount'),
            duration = item['contentDetails']['duration'],
            thumbnail = item['snippet']['thumbnails']['default']['url'],
            caption_status = item['contentDetails']['caption']
                )
            
            video_data.append(data)
    
    return video_data


def get_comment_details(video_ids):
    comment_details = []
    try:
        for i in video_ids:
            request = youtube.commentThreads().list(
            part = "snippet",
            videoId = i,
            maxResults = 50
                )

            response = request.execute()

            for item in response['items']:
                data = dict(
                comment_id = item['id'],
                video_id = item['snippet']['videoId'],
                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_published_date = item['snippet']['topLevelComment']['snippet']['publishedAt']             
                )

                comment_details.append(data)
    except:
        pass
    
    return comment_details


def get_playlist_details(channel_id):
    
    playlist_details = []
    next_page_token = None
    
    while True:
        request = youtube.playlists().list(
                    part = "snippet, contentDetails",
                    channelId = channel_id,
                    maxResults = 50,
                    pageToken = next_page_token
                    )

        response = request.execute()

        for i in response['items']:
            data = dict(
            playlist_id = i['id'],
            channel_id = i['snippet']['channelId'],
            playlist_name = i['snippet']['title'],
            video_count = i['contentDetails']['itemCount']
            )

            playlist_details.append(data)

        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break

    return playlist_details


client = MongoClient()

hostname = 'localhost'
port = 27017  # Default MongoDB port

# Create a MongoClient instance
client = MongoClient(hostname, port)

db = client['Youtube_Data']
collection = db['Channel_Information']



def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = video_id(channel_id)
    vi_details = get_video_information(vi_ids)
    com_details = get_comment_details(vi_ids)
    
    collection.insert_one({
        "channel_details": ch_details,
        "playlist_details":pl_details,
        "video_details": vi_details,
        "comments_details" : com_details
    })
    
    return "Uploaded successfully"
    

mydb = mysql.connector.connect(
host="localhost",
user="root",
password="1996",
auth_plugin="mysql_native_password"
)
cursor = mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS Youtube")
cursor.execute("use Youtube")
    


def create_channels_table():
    create_query = ('''create table if not exists channels (
                       channel_name varchar(100),
                       channel_id varchar(100) primary key,
                       subscribers bigint,
                       views bigint,
                       total_videos int,
                       channel_description text,
                       playlist_id varchar(100)
                       )'''
        )

    cursor.execute(create_query)
    mydb.commit()

    ch_list = []
    for ch_data in collection.find({}, {"_id" : 0, "channel_details": 1}):
        ch_list.append(ch_data['channel_details'])
        
    df = pd.DataFrame(ch_list)
    
    for index, row in df.iterrows():
        insert_query = '''insert into channels(
                            channel_name,
                            channel_id,
                            subscribers,
                            views,
                            total_videos,
                            channel_description,
                            playlist_id
                            )
                            values(%s, %s, %s, %s, %s, %s, %s)'''

        values = (row['channel_name'],
                 row['channel_id'],
                 row['subscribers'],
                 row['views'],
                 row['total_videos'],
                 row['channel_description'],
                 row['playlist_id'])

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")

def create_playlist_table():
    create_query = ('''create table if not exists playlists (
                               playlist_id varchar(255) primary key,
                               channel_id varchar(255),
                               playlist_name varchar(255),
                               video_count int,
                               FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
                               )
                               '''
                )

    cursor.execute(create_query)
    mydb.commit()
    
    pl_list = []
    for pl_data in collection.find({}, {"_id" : 0, "playlist_details": 1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])

    df_playlist = pd.DataFrame(pl_list)
    
    for index, row in df_playlist.iterrows():    
        insert_query = '''insert into playlists(
                            playlist_id,
                            channel_id,
                            playlist_name,
                            video_count
                            )
                            values(%s, %s, %s, %s)'''

        values = (
                 row['playlist_id'],
                 row['channel_id'],
                 row['playlist_name'],
                 row['video_count']
                 )
        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")


def create_video_table():
    try:
        create_query = ('''create table if not exists videos (
                           video_id varchar(255) primary key,
                           channel_id varchar(255),
                           video_name varchar(255),
                           published_date varchar(255),
                           view_count int,
                           like_count int,
                           favourite_count int,
                           comment_count int,
                           duration varchar(100),
                           thumbnail varchar(255),
                           caption_status varchar(255)
                           )
                           '''
            )

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Table already created")
        
    vi_list = []
    for vi_data in collection.find({}, {"_id" : 0, "video_details": 1}):
        for i in range (len(vi_data['video_details'])):
            vi_list.append(vi_data['video_details'][i])

    df_video = pd.DataFrame(vi_list)
    
    for index, row in df_video.iterrows():    
        insert_query = '''insert into videos(
                            video_id,
                            channel_id,
                            video_name,
                            published_date,
                            view_count,
                            like_count,
                            favourite_count,
                            comment_count,
                            duration,
                            thumbnail,
                            caption_status
                            )
                            values(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
                 row['video_id'],
                 row['channel_id'],
                 row['video_name'],
                 row['published_date'],
                 row['view_count'],
                 row['like_count'],
                 row['favourite_count'],
                 row['comment_count'],
                 row['duration'],
                 row['thumbnail'],
                 row['caption_status'],
                 )

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")


def create_comment_table():
    try:
        create_query = ('''create table if not exists comments (
                           comment_id varchar(255) primary key,
                           video_id varchar(255),
                           comment_text text,
                           comment_author varchar(255),
                           comment_published_date varchar(255),
                           FOREIGN KEY (video_id) REFERENCES videos(video_id)
                           )
                           '''
            )

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Table already created")
        
    cm_list = []
    
    for cm_data in collection.find({}, {"_id" : 0, "comments_details": 1}):
        for i in range (len(cm_data['comments_details'])):
            cm_list.append(cm_data['comments_details'][i])

    df_comments = pd.DataFrame(cm_list)
    
    for index, row in df_comments.iterrows():    
        insert_query = '''insert into comments(
                            comment_id,
                            video_id,
                            comment_text,
                            comment_author,
                            comment_published_date
                            )
                            values(%s, %s, %s, %s, %s)'''

        values = (
                 row['comment_id'],
                 row['video_id'],
                 row['comment_text'],
                 row['comment_author'],
                 row['comment_published_date'],
                 )

        try: 
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Values already entered")

def create_tables():
    create_channels_table()
    create_playlist_table()
    create_video_table()
    create_comment_table()
    
    return "Tables Created Successfully"


def get_channel_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']

    ch_list = []
    for ch_data in collection.find({}, {"_id" : 0, "channel_details": 1}):
        ch_list.append(ch_data['channel_details'])

    df1 = st.dataframe(ch_list)
    


def get_playlist_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']
    
    pl_list = []
    for pl_data in collection.find({}, {"_id" : 0, "playlist_details": 1}):
        for i in range(len(pl_data['playlist_details'])):
            pl_list.append(pl_data['playlist_details'][i])

    df2 = st.dataframe(pl_list)



def get_comment_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']

    cm_list = []
    for cm_data in collection.find({}, {"_id" : 0, "comments_details": 1}):
        for i in range (len(cm_data['comments_details'])):
            cm_list.append(cm_data['comments_details'][i])

    df3 = st.dataframe(cm_list)


def get_video_table():
    db = client['Youtube_Data']
    collection = db['Channel_Information']
    
    vi_list = []
    for vi_data in collection.find({}, {"_id" : 0, "video_details": 1}):
        for i in range (len(vi_data['video_details'])):
            vi_list.append(vi_data['video_details'][i])

    df4 = st.dataframe(vi_list)


#Streamlit part

with st.sidebar:
    st.title(":red[Youtube Data Harvesting and Warehousing]")
    st.header(":blue[Skill Take Away]")
    st.caption(":grey[Python Scripting]")
    st.caption(":grey[Data Collection]")
    st.caption(":grey[Mongo db]")
    st.caption(":grey[MYSQL Database]")
    st.caption(":grey[API Integration]")
    st.caption(":grey[Data Management]")
    
channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store Data"):
    ch_ids = []
    
    db = client['Youtube_Data']
    collection = db['Channel_Information']
    
    for ch_data in collection.find({}, {"_id" : 0, "channel_details": 1}):
        ch_ids.append(ch_data['channel_details']['channel_id'])
    
    if channel_id in ch_ids:
        st.success("Channel details already exixts")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    table = create_tables()
    st.success(table)

options = ['Channels', 'Playlists', 'Videos', 'Comments']
selected_option = st.radio('Select an option:', options)

if selected_option == 'Channels':
    get_channel_table()
    
if selected_option == 'Playlists':
    get_playlist_table()
    
if selected_option == 'Videos':
    get_video_table()
    
if selected_option == 'Comments':
    get_comment_table()
    

#sql connector
mydb = mysql.connector.connect(
host="localhost",
user="root",
password="1996",
auth_plugin="mysql_native_password"
)

cursor = mydb.cursor()
cursor.execute("use Youtube")


# Define the options for the select box
options = [
    "1. All the videos and the channel name",
    "2. channels with most number of videos",
    "3. 10 most viewed videos",
    "4. comments in each videos",
    "5. Videos with highest likes",
    "6. likes of all videos",
    "7. views of each channel",
    "8. videos published in the year of 2022",
    "9. average duration of all videos in each channel",
    "10. videos with highest number of comments"
]

# Use st.selectbox to create a select box
question = st.selectbox("Select your question", options, key='selectbox_question')

if question == "1. All the videos and the channel name" :
    query_1 = '''select video_name, channel_name from videos v
                    join channels c on
                    c.channel_id = v.channel_id
                    order by channel_name;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df1= pd.DataFrame(t1, columns = ['video name', 'channel name'])
    st.write(df1)

elif question == "2. channels with most number of videos" :
    query_1 = '''SELECT channel_name, total_videos
FROM channels
order by total_videos desc;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df2 = pd.DataFrame(t1, columns = ['channel name', 'total videos'])
    st.write(df2)
    
elif question == "3. 10 most viewed videos" :
    query_1 = '''select video_name, view_count from videos
order by view_count desc
limit 10;'''
    
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df3 = pd.DataFrame(t1, columns = ['video name', 'view count'])
    st.write(df3)
    
elif question == "4. comments in each videos" :
    query_1 = '''select video_name, comment_count from videos
order by comment_count desc;
'''
    
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df4 = pd.DataFrame(t1, columns = ['video name', 'comment count'])
    st.write(df4)
    
elif question == "5. Videos with higest likes" :
    query_1 = '''select channel_name, like_count from videos v
join channels c on 
c.channel_id = v.channel_id
where like_count is not null and like_count != 0
order by like_count desc;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df5 = pd.DataFrame(t1, columns = ['channel name', 'like count'])
    st.write(df5)
    
elif question == "6. likes of all videos" :
    query_1 = '''select video_name, like_count from videos
where like_count is not null;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df6 = pd.DataFrame(t1, columns = ['video name', 'like count'])
    st.write(df6)
    
elif question == "7. views of each channel" :
    query_1 = '''select channel_name, views from channels
order by channel_name;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df7 = pd.DataFrame(t1, columns = ['channel name', 'total views'])
    st.write(df7)
    
elif question == "8. videos published in the year of 2022" :
    query_1 = '''select video_name, published_date from videos
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df8 = pd.DataFrame(t1, columns = ['video name', 'published date'])
    st.write(df8)
    
elif question == "9. average duration of all videos in each channel" :
    query_1 = '''select video_name, duration from videos
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df9 = pd.DataFrame(t1, columns = ['video name', 'duration'])
    st.write(df9)
    
elif question == "10. videos with highest number of comments" :
    query_1 = '''select channel_name, video_name, comment_count from videos v
join channels c on 
v.channel_id = c.channel_id
order by comment_count desc;
                    '''
    cursor.execute(query_1)
    t1 = cursor.fetchall()
    df10 = pd.DataFrame(t1, columns = ['channel name', 'video name', 'comment count'])
    st.write(df10)
    

