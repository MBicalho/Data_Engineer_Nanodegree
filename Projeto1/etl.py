import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
"""
Function that will go through the data json files and transform them into a dataframe, 
after that it will be responsible for inserting this data inside our tables.

Função que irá percorrer os arquivos de dados json, e transformá-los em um dataframe,
depois disso será responsável por inserir os dados nas tabelas artist e song.

Arguments: 
        cur -> cursor object.
        filepath -> file path of the song or artist data.
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
   

    # insert song record
    song_data = song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    
"""
Function that will go through the log json files, convert the timestamp column and transform them into a dataframe, 
after that it will be responsible for inserting data in time and users tables.

It is also responsible for fetching the artist_id and song_id and then inserting them into the songplays table.

Função que irá percorrer os arquivos de log json, converter a coluna timestamp e transformá-los em um dataframe,
depois disso será responsável por inserir os dados nas tabelas time e users.

Ela também é responsável por buscar o artist_id e song_id e então inseri-los na tabela songplays.

Arguments: 
        cur -> cursor object.
        filepath -> file path of the log data.
"""    

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit = 'ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data = df[['userId', 'firstName', 'lastName', 'gender', 'level']].values.tolist()
    column_labels = ['user_id', 'firstName', 'lastName', 'gender', 'level']
    user_df = pd.DataFrame(user_data, columns=column_labels)

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, artistid, row.sessionId, songid, row.location, row.userAgent, row.level)
        cur.execute(songplay_table_insert, songplay_data)

        """
        This function is responsible for capturing all the files in the directory and then
        performs the execution of each function responsible for inserting data into our database.
        
        Essa função é responsável por capturar todos os arquivos do diretório e depois
        realiza a execução de cada função responsável por inserir os dados em nosso banco de dados.
        
        Arguments:
                cur -> cursor object
                conn -> connection object
                filepath -> file path of the data or log json
                func -> the function to be performed 
        
        """

def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()