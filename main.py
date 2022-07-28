import requests
import pandas as pd
import time
import sqlalchemy
from sqlalchemy import create_engine
from prefect import task,Flow

from prefect.schedules import Schedule, filters, clocks
from datetime import timedelta
def video_stats(video_id):
    API_key = "AIzaSyB-HID1uSEprUcTAGpYf374CZTKrAbZJJA"
    video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+API_key
    res2= requests.get(video_stats).json()
    view_count = res2['items'][0]['statistics']['viewCount']
    like_count = res2['items'][0]['statistics']['likeCount']
  #dislike_count = res2['items'][0]['statistics']['dislikeCount']
    comment_count = res2['items'][0]['statistics']['commentCount']
    return view_count, like_count,  comment_count

@task(nout=1)
def get_videos():
    API_key="AIzaSyB-HID1uSEprUcTAGpYf374CZTKrAbZJJA"
    channel_id="UC2ri4rEb8abnNwXvTjg5ARw"
    # build our dataframe
    data = pd.DataFrame(
        columns=["video_id", "video_title", "video_channel", "publish_date", "views", "like_count", "comments"])
    pageToken = ""
    while 1:
        url = "https://www.googleapis.com/youtube/v3/search?key=" + API_key + "&channelId=" + channel_id + "&part=snippet,id&order=date&maxResults=10000&" + pageToken

        response = requests.get(url).json()
        print(response)
        time.sleep(1)  # give it a second before starting the for loop
        for video in response['items']:
            if video['id']['kind'] == "youtube#video":
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                channel_title = video['snippet']['channelTitle']

                publish_date = video['snippet']['publishedAt']

                view_count, like_count, comment_count = video_stats(video_id)

                data = data.append({'video_id': video_id, 'video_title': video_title, "video_channel": channel_title,
                                    "publish_date": publish_date, "views": view_count,
                                    "like_count": like_count,
                                    "comments": comment_count}, ignore_index=True)
        try:
            if response['nextPageToken'] != None:  # none means we have reached the last page
                pageToken = "pageToken=" + response['nextPageToken']

        except:
            break

    return data


@task
def postgres_conn(data):
    data.drop(data.columns[data.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

    engine = create_engine('postgresql://root:root@localhost:5432/education')
    engine.connect()
    pd.io.sql.get_schema(data, name="khanAcademyData", con=engine)
    data.head(n=0).to_sql(name="khanAcademyData", con=engine, if_exists="replace")
    data.to_sql(name="khanAcademyData", con=engine, if_exists="append")
schedules= Schedule(
# fire every hour
    clocks=[clocks.IntervalClock(timedelta(hours=24))],
    # but only on weekdays
    filters=[filters.is_weekday])

with Flow("Batch-datapipeline") as flow:
    data = get_videos()
    postgres_conn(data)

if __name__ == "__main__":
    schedules = Schedule(
        # fire every hour
        clocks=[clocks.IntervalClock(timedelta(hours=24))],
        # but only on weekdays
        filters=[filters.is_weekday])
    flow.schedule = schedules
    flow.run()
    flow.register(project_name="Batch-pipeline")



