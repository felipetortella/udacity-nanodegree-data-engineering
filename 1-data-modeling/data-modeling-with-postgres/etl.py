import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """Process a song file extracting song data and artist data

    Parameters:
    cur (Cursor): Cursor of the database connection
    filepath (String): File path of the .json file to be extracted

    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # create new song dataframe from df
    song_df = df[['song_id','title','artist_id','year','duration']]
    song_df.drop_duplicates()
    song_df_value = song_df.values    
    
    # insert song record
    song_data = song_df_value[0].tolist()
    #print(song_data)
    cur.execute(song_table_insert, song_data)
    
    # create new artist dataframe from df
    artist_df = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_df.drop_duplicates()
    artist_df_value = artist_df.values
    # insert artist record
    artist_data = artist_df_value[0].tolist()
    print(artist_data)
    print(artist_table_insert)
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """Process a log file extracting time data, user data and song play data

    Parameters:
    cur (Cursor): Cursor of the database connection
    filepath (String): File path of the .json file to be extracted

    """
    print(filepath)
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts']) 
    
    # insert time data records
    time_df = df[['ts']].copy()
    time_df['hour'] = time_df['ts'].dt.hour
    time_df['day'] = time_df['ts'].dt.day
    time_df['weekofyear'] = time_df['ts'].dt.weekofyear
    time_df['month'] = time_df['ts'].dt.month
    time_df['year'] = time_df['ts'].dt.year
    time_df['weekday'] = time_df['ts'].dt.weekday

    time_df.rename({'ts': 'start_time'}, axis=1, inplace=True)
    time_df = time_df.drop_duplicates()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    
    user_df =  df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts) , row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
def process_data(cur, conn, filepath, func):
    """Process a log file extracting time data, user data and song play data

    Parameters:
    cur (Cursor): Cursor of the database connection
    conn (Connection): Connection from psycopg2.connect
    filepath (String): Folder path to be used
    func (String): Function to be used to process the data
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main process of ETL. It will process log and song data"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()