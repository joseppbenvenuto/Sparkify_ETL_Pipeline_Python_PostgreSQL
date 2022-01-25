import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


# insert single record of song & artist data
def process_song_file(cur, filepath):
    '''
    - Processes one JSON file at a time and converts them into data frames. 
    - Once JSON file is converted into a data frame it's normalized into second normal form 
    - The song and artist data frames are inserted into their corresponding tables found in the Sparkify database.
    '''
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


# convert ts column to datetime
# create & insert time(filtered for NextSong) & user data
# creates & inserts songplay data
def process_log_file(cur, filepath):
    '''
     - Processes one JSON file at a time and converts them into data frames.
     - The process_log_file function converts data & time data from milliseconds to hours, days, weeks, months, years, and weekdays
     - The page column is filtered for NextSong. 
     - From the first data frame, it's normalized into second normal form so that user, time, and fact data are in separate tables
     - Once all three tables are created from the single JASON file processed, they inserted into their corresponding tables in the Sparkify 
       database.
    '''
    # open log file
    df = pd.read_json(filepath, lines = True)
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    # Convert ts columns in milliseconds to datetime
    df['start_time'] = pd.to_datetime(df['ts'], unit = 'ms')

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')
    t['hour'], t['day'], t['week_of_year'], t['month'], t['year'],\
    t['weekday'] = t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month,\
    t.dt.year, t.dt.weekday

    # insert time data records
    time_data = (list(zip(t, t['hour'], t['day'], t['week_of_year'], t['month'], t['year'], t['weekday'])))
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns = column_labels)

    time_df['start_time'] = pd.to_datetime(time_df['start_time'])

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(users_table_insert, row)


    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        # assign song_id and artist_id to given values
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert data into songplay table
        songplay_data = (row.start_time, row.userId, row.level, song_id,
                         artist_id, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


# process all data
def process_data(cur, conn, filepath, func):
    '''
    The process_data functions loops through all data files and applies the process_song_file & process_log_file functions to process and 
    insert that data into their corresponding tables in the Sparkify database.
    '''
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
        print('{}/{} files processed'.format(i, num_files))


# run all functions
def main():
    '''
    The main function creates the Sparkify database connection and uses the above functions to successfully insert all data to their 
    corresponding tables in the Sparkify database.
    '''
    conn = psycopg2.connect('''host=localhost
                               dbname=sparkifydb
                               user=postgres
                               password=rootroot''')
    cur = conn.cursor()

    process_data(cur, conn, filepath = 'data/song_data', func = process_song_file)

    process_data(cur, conn, filepath = 'data/log_data', func = process_log_file)

    conn.close()


if __name__ == "__main__":
    main()