import  pandas as pd
import os
import  glob
import psycopg2
from sql_queries import  *

def process_data(cur, con, filepath, func):
    """
    Driver function to load data from songs and event logs  files into postgreSQL
    :param cur: a database cursor reference
    :param con: database connection reference
    :param filepath : parent directory  where the file exist
    :param func = function to call
    :return:
    """
    all_files = list()
    for root, dirs, files, in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))
    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format( num_files, filepath))
    for i, datafile in enumerate(all_files,1):
        func(cur, datafile)
        con.commit()
        print("{}/{} file processed".format(i, num_files))


def process_song_file(cur, datafile):
    """

    :param cur: connection to database
    :param datafile: list contains filepath
    """
    df = pd.DataFrame([pd.read_json(datafile, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        # insert into table artist
        artists_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artists_data)


       # insert into table  song
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    print(f"Records inserted for files {datafile}")


def process_log_file(cur, datafile):
    """

    :param cur: connection to database
    :param datafile: list contains filepath
    """
    # open log file
    df = pd.read_json(datafile,lines= True)
    df.columns = [ c.lower() for c in df.columns]

    # filter by next song action
    df = df[df['page']=='NextSong'].astype({'ts': 'datetime64[ms]'})

    #convert timestamp  column to datetime
    t = pd.Series(df['ts'], index = df.index)
    # insert time  data records
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_data = []
    for data in t:
        time_data.append([data, data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user tables
    user_df = df[['userid','firstname', 'lastname','gender','level']]

    for i, row in user_df.iterrows():
        # insert user tables
        cur.execute(user_table_insert,list(row))

    # insert songplay records
    for index, row in df.iterrows():
        cur.execute(song_select,(row.song, row.artist, row.length))
        result = cur.fetchone()
        if result:
            songid,artistid= result
        else:
            songid, artistid= None, None

        # insert songplay record
        songplay_data =(row.ts,row.userid, row.level, songid,artistid, row.sessionid, row.location, row.useragent)
        cur.execute(songplay_table_insert, songplay_data)


def main():
    """
    function for loading song and log data into  postgres database
    """
    con = psycopg2.connect(host='127.0.0.1', dbname='sparkifydb', user='postgres', password='juhari123')
    cur = con.cursor()

    process_data(cur,con, filepath='data/song_data', func= process_song_file)
    process_data(cur, con, filepath='data/log_data', func=process_log_file)

    con.close()

if __name__ == "__main__":
    main()
    print("Finished Processing !!")








