from sql_queries import  create_table_queries, drop_table_queries

import  psycopg2
def create_database():
    #connect to postgresql
    con = psycopg2.connect(host= '127.0.0.1', dbname ='postgres', user ='postgres', password='juhari123')
    con.set_session(autocommit = True)
    cur = con.cursor()

    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute ("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")


    # close connection to default database
    con.close()
    # connect to sparkifydb
    con = psycopg2.connect(host= '127.0.0.1', dbname ='sparkifydb', user ='postgres', password='juhari123')
    cur = con.cursor()
    return  cur, con

def drop_tables(cur, con):
    for query in drop_table_queries:
        cur.execute(query)
        con.commit()

def create_tables(cur, con):
    for query in create_table_queries:
        cur.execute(query)
        con.commit()

def main():

    cur, con = create_database()
    drop_tables(cur, con)
    print('Tables dropped succesfully')
    create_tables(cur, con)
    print('Table Created Succesfully')

if __name__ == "__main__":
    main()

