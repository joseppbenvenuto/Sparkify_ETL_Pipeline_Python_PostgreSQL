import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

'''

An additional function (process_data_log) was used for the log data to allow for a continuous index between the different log data files.
Originally the log data files were processed one at a time as opposed to bulk and as a result the indexes would reset and not be captured by the songplay_id field, therefore not returning any matching values for song_id & artist_id.

My ETL pipeline deviates slightly from the original, however, it addresses the bug (resetting indexing and not capturing full data when inserting values into the songplay table) existing within the original. 

'''


# insert single record of song & artist data
def process_song_file(cur, filepath):
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
# craet & insert songplay data
def process_log_file(cur, filepath, index):
    # open log file
    df = pd.read_json(filepath, lines = True)
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    df['new_index'] = index

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

    # Convert ts columns in milliseconds to datetime
    df['start_time'] = pd.to_datetime(df['ts'], unit = 'ms')

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
        songplay_data = (row.new_index, row.start_time, row.userId, row.level, song_id,
                         artist_id, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


# process all song & artist data files
def process_data_song_artist(cur, conn, filepath, func):
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


# process all log data files
def process_data_log(cur, conn, filepath, func):
    # set index_start to the largest songplay_id to load new files starting from the latest songplay_id
    cur.execute(find_index)
    index_start = cur.fetchone()[0]

    if index_start  == None:
        index_start = 0
        print('starting songplay_id: ' + str(index_start))
    else:
        print('starting songplay_id: ' + str(index_start))
        pass
    
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
    index_sum = index_start
    for i, datafile in enumerate(all_files, 1):
        df = pd.read_json(datafile, lines = True)
        df = df.loc[df['page'] == 'NextSong']
        # create continuous index for songplay table
        index = list(range(index_sum + 1, index_sum + df.shape[0] + 1))
        func(cur, datafile, index)
        conn.commit()
        index_sum += df.shape[0]
        print('{}/{} files processed.'.format(i, num_files))


# run all functions
def main():
    conn = psycopg2.connect('''host=localhost
                               dbname=sparkifydb
                               user=postgres
                               password=rootroot''')
    cur = conn.cursor()

    process_data_song_artist(cur, conn, filepath = 'data/song_data', func = process_song_file)

    process_data_log(cur, conn, filepath = 'data/log_data', func = process_log_file)

    conn.close()


if __name__ == "__main__":
    main()