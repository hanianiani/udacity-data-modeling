import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process an individual .json file containing song and artist metadata.
    
    Keyword arguments:
    cur -- the database cursor object
    filepath -- a string indicating the path to the metadata file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_df = pd.DataFrame(
        df,columns=['song_id','title','artist_id','year','duration']
    ) 
    song_data = list(song_df.values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_df = pd.DataFrame(
        df,columns=['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    )
    artist_data = list(artist_df.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Process an individual .json file containing user activity logs
    
    Keyword arguments:
    cur -- the database cursor object
    filepath -- a string indicating the path to the metadata file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    t = df['ts']
    
    # insert time data records
    time_data = (t,
             t.dt.hour,
             t.dt.day,
             t.dt.weekofyear,
             t.dt.month,
             t.dt.year,
             t.dt.weekday
                )
    column_labels = ('start_time',
                'hour',
                'day',
                'week',
                'month',
                'year',
                'weekday'
                    )
    time_df = pd.DataFrame.from_dict(
        dict(zip(column_labels, time_data))
    )
    
    #pre-clean data in time_df
    time_df = time_df.drop_duplicates()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df,columns=['userId','firstName','lastName','gender','level'])
    
    # pre-clean data in user_df
        #drop duplicate records
    user_df = user_df.drop_duplicates()
        #drop records where userId is None
    user_df = user_df.dropna(subset=['userId'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get artistid and songid from artists and songs tables
        cur.execute(song_select, (row.artist, row.song, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)  
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iterate over all files in a directory and process them with a function
    
    Keyword arguments:
    cur -- the database cursor object
    conn -- the database connection object
    filepath -- a string indicating the directory where the files are
    func -- the function with which the files should be processed
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
    
    # connect to the database    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # process files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()