# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 20:09:55 2023

@author: ppanchum
@author: jmaddipo -> Added distributed indexing and query optimizations.

"""

import psycopg2
from psycopg2 import extras
import psycopg2.extensions
import hashlib
import time
import random
import string

DATABASE_NAME = 'project'
child1 = 'child1'
child2 = 'child2'


def create_database(dbname):
    """Connect to PostgreSQL and create a database named {DATABASE_NAME}"""
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="test",
        port=5432
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Preparing query to create a database
    create_query = f"CREATE DATABASE {dbname};"
    
    
    # Creating a database
    cursor.execute(create_query)
    print("Database created successfully........")

    # Closing the connection
    cursor.close()

def create_database_child(child1):
    """Connect to PostgreSQL and create a database named {DATABASE_NAME}"""
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="test",
        port=5432
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Preparing query to create a database
    create_query = f"CREATE DATABASE {child1};"
    
    
    # Creating a database
    cursor.execute(create_query)
    print("Database created successfully........")

    # Closing the connection
    cursor.close()
    
def create_database_child2(child2):
    """Connect to PostgreSQL and create a database named {DATABASE_NAME}"""
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="test",
        port=5432
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Preparing query to create a database
    create_query = f"CREATE DATABASE {child2};"
    
    
    # Creating a database
    cursor.execute(create_query)
    print("Database created successfully........")

    # Closing the connection
    cursor.close()

def connect_postgres(dbname):
    """Connect to PostgreSQL using psycopg2 with the specified database name"""
    return psycopg2.connect(
        host="localhost",
        database=dbname,
        user="postgres",
        password="test",
        port=5432
    )

def create_table(conn):
    
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS UserProfile (
            UserID SERIAL PRIMARY KEY,
            UserName TEXT,
            Location TEXT,
            Email TEXT
            -- Other user attributes
        )
    ''')
    
    # Create Post table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Post (
            PostID SERIAL PRIMARY KEY,
            UserID INTEGER,
            Content TEXT,
            Timestamp TIMESTAMP,
            Likes INTEGER,
            Shares INTEGER,
            -- Other post attributes
            FOREIGN KEY (UserID) REFERENCES UserProfile(UserID)
        )
    ''')
    
    # Create Comment table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Comment (
            CommentID SERIAL PRIMARY KEY,
            PostID INTEGER,
            UserID INTEGER,
            Content TEXT,
            Timestamp TIMESTAMP,
            -- Other comment attributes
            FOREIGN KEY (PostID) REFERENCES Post(PostID),
            FOREIGN KEY (UserID) REFERENCES UserProfile(UserID)
        )
    ''')

    # Create Friendship table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Friendship (
            UserID1 INTEGER,
            UserID2 INTEGER,
            PRIMARY KEY (UserID1, UserID2),
            FOREIGN KEY (UserID1) REFERENCES UserProfile(UserID),
            FOREIGN KEY (UserID2) REFERENCES UserProfile(UserID)
        )
    ''')
   
    cursor.close()

def create_table1(conn):
    
    cursor = conn.cursor()
    
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS UserProfile (
            ID SERIAL PRIMARY KEY,
            UserID INTEGER UNIQUE,
            UserName TEXT,
            Location TEXT,
            Email TEXT
            -- Other user attributes
        )
    ''')
    
    # Create Post table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Post (
            ID SERIAL PRIMARY KEY,
            PostID INTEGER,
            UserID INTEGER,
            Content TEXT,
            Timestamp TIMESTAMP,
            Likes INTEGER,
            Shares INTEGER,
            -- Other post attributes
            FOREIGN KEY (UserID) REFERENCES UserProfile(UserID)
        )
    ''')
    cursor.close()    

def print_data_in_child(conn_child1):
    table_names = ['UserProfile', 'Friendship','Post', 'Comment','userprofile_tempe','PostLikesShares']

    with conn_child1.cursor() as cursor_child1:
        for table_name in table_names:
            print(f"\nData in {table_name} table:")
            cursor_child1.execute(f"SELECT * FROM {table_name}")
            rows = cursor_child1.fetchall()

            if not rows:
                print("No data found.")
            else:
                column_names = [desc[0] for desc in cursor_child1.description]
                print(column_names)
                for row in rows:
                    print(row)    
                    
def print_data_in_child1(conn_child1):
    table_names = ['UserProfile','Post']

    with conn_child1.cursor() as cursor_child1:
        for table_name in table_names:
            print(f"\nData in {table_name} table:")
            cursor_child1.execute(f"SELECT * FROM {table_name}")
            rows = cursor_child1.fetchall()

            if not rows:
                print("No data found.")
            else:
                column_names = [desc[0] for desc in cursor_child1.description]
                print(column_names)
                for row in rows:
                    print(row) 
                    
def add_user(conn, username, location, email):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO UserProfile (UserName, Location, Email) VALUES (%s, %s, %s) RETURNING UserID",
                       (username, location, email))
        user_id = cursor.fetchone()[0]

    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            cursor.execute("INSERT INTO UserProfile ( UserID,UserName, Location, Email) VALUES ( %s,%s, %s, %s)",
                           (user_id, username, location, email))
    else:
        with conn_child2.cursor() as cursor:
            cursor.execute("INSERT INTO UserProfile ( UserID,UserName, Location, Email) VALUES ( %s,%s, %s, %s)",
                           (user_id, username, location, email))

def add_post(conn, user_id, content,likes,share):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO Post ( UserID, Content, Timestamp, Likes , Shares) VALUES (%s, %s, NOW(),%s,%s) RETURNING PostID",
                       ( user_id, content,likes,share))
        post_id = cursor.fetchone()[0]
        
        
    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            cursor.execute("INSERT INTO Post ( PostID,UserID, Content, Timestamp, Likes , Shares) VALUES (%s,%s, %s, NOW(),%s,%s)",
                           ( post_id,user_id, content,likes,share))
    else:
        with conn_child2.cursor() as cursor:
            cursor.execute("INSERT INTO Post ( PostID,UserID, Content, Timestamp, Likes , Shares) VALUES (%s,%s, %s, NOW(),%s,%s)",
                           ( post_id,user_id, content,likes,share))

def add_comment(conn, post_id, user_id, content):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO Comment ( PostID, UserID, Content, Timestamp) VALUES (%s, %s, %s, NOW())",
                       ( post_id, user_id, content))

def add_friendship(conn, user_id1, user_id2):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO Friendship (UserID1, UserID2) VALUES (%s, %s)", (user_id1, user_id2))
        cursor.execute("INSERT INTO Friendship (UserID1, UserID2) VALUES (%s, %s)", (user_id2, user_id1))

def delete_user(conn, user_id, user_name):
    with conn.cursor() as cursor:
        # Delete friendships involving the user
        cursor.execute("DELETE FROM Friendship WHERE UserID1 = %s OR UserID2 = %s", (user_id, user_id))
        # Delete comments of the user
        cursor.execute("DELETE FROM Comment WHERE UserID = %s", (user_id,))
        # Delete posts of the user (and associated comments)
        cursor.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
        # Delete the user
        cursor.execute("DELETE FROM UserProfile WHERE UserID = %s", (user_id,))
        
        
    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            # Delete posts of the user (and associated comments)
            cursor.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
            # Delete the user
            cursor.execute("DELETE FROM UserProfile WHERE UserID = %s", (user_id,))
    else:
        with conn_child2.cursor() as cursor:
            # Delete posts of the user (and associated comments)
            cursor.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
            # Delete the user
            cursor.execute("DELETE FROM UserProfile WHERE UserID = %s", (user_id,))
     
def delete_post(conn, post_id,user_id):
    with conn.cursor() as cursor:
        # Delete comments under the post
        cursor.execute("DELETE FROM Comment WHERE PostID = %s", (post_id,))
        # Delete the post
        cursor.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))
        
        
    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            cursor.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))
    else:
        with conn_child2.cursor() as cursor:
            cursor.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))

def delete_comment(conn, comment_id):
    with conn.cursor() as cursor:
        # Delete the comment
        cursor.execute("DELETE FROM Comment WHERE CommentID = %s", (comment_id,))

def delete_friendship(conn, user_id1, user_id2):
    with conn.cursor() as cursor:
        # Delete the friendship in both directions
        cursor.execute("DELETE FROM Friendship WHERE (UserID1 = %s AND UserID2 = %s) OR (UserID1 = %s AND UserID2 = %s)",
                       (user_id1, user_id2, user_id2, user_id1))          
        
def update_user(conn, user_id, new_username, new_location, new_email):
    with conn.cursor() as cursor:
        cursor.execute("UPDATE UserProfile SET UserName = %s, Location = %s, Email = %s WHERE UserID = %s",
                       (new_username, new_location, new_email, user_id))
        
        
        hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
        hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
        
        conn_child2 = psycopg2.connect(
            host="localhost",
            database="child2",
            user="postgres",
            password="test",
            port=5432
        )
        conn_child2.autocommit = True

        
        conn_child1 = psycopg2.connect(
            host="localhost",
            database="child1",
            user="postgres",
            password="test",
            port=5432
        )
        conn_child1.autocommit = True

        if hash_integer % 2 == 0:
            with conn_child1.cursor() as cursor:
                cursor.execute("UPDATE UserProfile SET UserName = %s, Location = %s, Email = %s WHERE UserID = %s",
                               (new_username, new_location, new_email, user_id))
        else:
            with conn_child2.cursor() as cursor:
                cursor.execute("UPDATE UserProfile SET UserName = %s, Location = %s, Email = %s WHERE UserID = %s",
                               (new_username, new_location, new_email, user_id))

def update_post(conn, post_id, new_content,user_id):
    with conn.cursor() as cursor:
        cursor.execute("UPDATE Post SET Content = %s WHERE PostID = %s", (new_content, post_id))
    cursor.close()
    
    
    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="test",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            cursor.execute("UPDATE Post SET Content = %s WHERE PostID = %s", (new_content, post_id))
    else:
        
        with conn_child2.cursor() as cursor:
            cursor.execute("UPDATE Post SET Content = %s WHERE PostID = %s", (new_content, post_id))

def update_comment(conn, comment_id, new_content):
    
    
    with conn.cursor() as cursor:
        cursor.execute("UPDATE Comment SET Content = %s WHERE CommentID = %s", (new_content, comment_id))
        
def get_user_by_username(conn, username):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM UserProfile WHERE UserName = %s", (username,))
        return cursor.fetchone()

def get_all_users(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM UserProfile")
        return cursor.fetchall()

def get_friends_of_user(conn, username):
    with conn.cursor() as cursor:
        cursor.execute("SELECT UserProfile.* FROM UserProfile "
                       "JOIN Friendship ON (UserProfile.UserID = Friendship.UserID1 OR UserProfile.UserID = Friendship.UserID2) "
                       "WHERE UserProfile.UserName = %s", (username,))
        return cursor.fetchall()

def get_post_by_id(conn, post_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Post WHERE PostID = %s", (post_id,))
        return cursor.fetchone()

def get_all_posts_of_user(conn, user_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Post WHERE UserID = %s", (user_id,))
        return cursor.fetchall()

def get_comment_by_id(conn, comment_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Comment WHERE CommentID = %s", (comment_id,))
        return cursor.fetchone()

def get_all_comments_on_post(conn, post_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Comment WHERE PostID = %s", (post_id,))
        return cursor.fetchall()
    
def horizontal_fragmentation_by_region(conn, region):
    """Perform horizontal fragmentation based on the 'Location' attribute"""

    with conn.cursor() as cursor:
        # Create a new table for the specified region
        cursor.execute(f'DROP TABLE IF EXISTS UserProfile_{region.replace(" ", "_")}')
        cursor.execute(f'''
            CREATE TABLE UserProfile_{region.replace(' ', '_')} AS
            SELECT * FROM UserProfile WHERE Location = %s
        ''', (region,))
        
    cursor.close()
    
def vertical_fragmentation_likes_shares(conn):
    """Perform vertical fragmentation by separating Likes and Shares in the Post table"""

    with conn.cursor() as cursor:
        # Drop the table if it already exists
        cursor.execute('DROP TABLE IF EXISTS PostLikesShares')

        # Create a new table for Likes and Shares
        cursor.execute('''
            CREATE TABLE PostLikesShares AS
            SELECT PostID, Likes, Shares FROM Post
        ''')

        # Remove Likes and Shares columns from the original Post table
        cursor.execute('''
            ALTER TABLE Post
            DROP COLUMN IF EXISTS Likes,
            DROP COLUMN IF EXISTS Shares
        ''')

    conn.commit() 
    cursor.close()

#Optimization Script using EXPLAIN and EXPLAIN ANALYZE
def run_explain(query, conn):
    """Run EXPLAIN and EXPLAIN ANALYZE on a given query and print the results."""
    #DB_PARAMS = {
    #'database': 'project',
    #'user': 'postgres',
    #'password': 'pass',
    #'host': 'localhost',
    #'port': '5432'
    #}
    with psycopg2.connect(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {query}")
            explain_output = cur.fetchall()
            print("EXPLAIN Output:")
            for row in explain_output:
                print(row[0])

            cur.execute(f"EXPLAIN ANALYZE {query}")
            explain_analyze_output = cur.fetchall()
            print("\nEXPLAIN ANALYZE Output:")
            for row in explain_analyze_output:
                print(row[0])

#Performance Metrics:
def capture_performance_metrics(query, conn):
    """Execute a query and capture its performance metrics."""    
    with conn.cursor() as cur:
        start_time = time.time()
        cur.execute(query)
        duration = time.time() - start_time
        print(f"Query Time: {duration:.2f} seconds")
        return duration

def create_index(table_name, column_name, conn,  index_type="btree"):
    """Create an index on a specified column of a table."""
    index_name = f"{table_name}_{column_name}_{index_type}_idx"    
    with conn.cursor() as cur:
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING {index_type} ({column_name});")
        print(f"Index {index_name} created on {table_name}({column_name}).")

def refactor_query(conn, original_query, optimized_query):
    """Compare the performance of the original and optimized queries."""
    original_duration = capture_performance_metrics(original_query, conn)
    optimized_duration = capture_performance_metrics(optimized_query, conn)
    print(f"Original query duration: {original_duration:.2f} seconds")
    print(f"Optimized query duration: {optimized_duration:.2f} seconds")
    improvement = (original_duration - optimized_duration) / original_duration * 100
    print(f"Performance improvement: {improvement:.2f}%")

def generate_random_userprofiles(num_users):
    """Generate a list of tuples representing user profiles."""
    userprofiles_data = []
    for i in range(1, num_users + 1):
        username = ''.join(random.choices(string.ascii_lowercase, k=8))
        location = ''.join(random.choices(string.ascii_lowercase + ' ', k=10)).title()
        email = f"{username}@example.com"
        userprofiles_data.append((i, username, location, email))
    return userprofiles_data

def batch_insert(connection, base, table_name, data_list, batch_size=100):
    """Insert data in batches to improve performance."""
    with connection.cursor() as cur:
        # Prepare the SQL statement
        sql = f"INSERT INTO {table_name} (userid, username, location, email) VALUES (%s, %s, %s, %s)"
        # Execute the batch operation
        for i in range(base, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            extras.execute_batch(cur, sql, batch)
        # Commit the transaction
        connection.commit()
        print(f"Inserted {len(data_list)} records into {table_name}.")

def capture_performance_metrics_disable_cache(query, conn):
    """Execute a query and capture its performance metrics with high precision."""
    with conn.cursor() as cur:
        # Disable caching (note: this might not be supported or might require superuser privileges)
        cur.execute('DISCARD ALL;')
        
        # Use a high-resolution timer
        start_time = time.perf_counter()
        cur.execute(query)
        conn.commit()  # Ensure changes are committed if the query modifies data
        duration = time.perf_counter() - start_time
        print(f"Query Time: {duration:.6f} seconds")  # Note the increased precision
        return duration

def refactor_query_disable_cache(conn, original_query, optimized_query):
    """Compare the performance of the original and optimized queries."""
    with conn.cursor() as cur:
        cur.execute('BEGIN;')
        original_duration = capture_performance_metrics_disable_cache(original_query, conn)
        cur.execute('ROLLBACK;')  # Rollback to ensure the database is unchanged for the next query
        
        cur.execute('BEGIN;')
        optimized_duration = capture_performance_metrics_disable_cache(optimized_query, conn)
        cur.execute('ROLLBACK;')
    
    print(f"Original query duration: {original_duration:.6f} seconds")
    print(f"Optimized query duration: {optimized_duration:.6f} seconds")
    if original_duration > 0:  # Prevent division by zero
        improvement = (original_duration - optimized_duration) / original_duration * 100
        print(f"Performance improvement: {improvement:.2f}%")
    else:
        print("Insufficient resolution for meaningful performance comparison.")

def create_index_on_children(conn_params_list, table_name, column_name):
    for conn in conn_params_list:        
        with conn.cursor() as cur:
            cur.execute(f"CREATE INDEX ON {table_name} ({column_name});")
            conn.commit()
            print(f"Index on {column_name} created for {params['database']}")

def execute_query_on_children(conn_params_list, query):
    results = []
    for conn in conn_params_list:        
        with conn.cursor() as cur:
            start_time = time.time()
            cur.execute(query)
            result = cur.fetchall()
            duration = time.time() - start_time
            results.extend(result)
            #print(f"Query on {conn['database']} took {duration:.2f} seconds.")
    return results

def time_query(conn, query, params):
    """Time how long it takes to execute a query and fetch all results."""
    with conn.cursor() as cursor:
        start = time.time()
        cursor.execute(query, params)
        cursor.fetchall()
        return time.time() - start

def execute_query_on_all_dbs(dbs_params, query, params):
    """Execute a query on all databases and report the total time taken."""
    total_time = 0
    for conn in dbs_params:        
        duration = time_query(conn, query, params)
        total_time += duration
        print(f"Query took {duration:.4f} seconds on database ")
    return total_time

def create_index(conn, table_name, column_name):
    """Create an index on a specified column of a table."""
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{column_name} ON {table_name} ({column_name});")
        conn.commit()
        print(f"Created index on column '{column_name}' of table '{table_name}'.")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Create the 'Project' database
    #create_database(DATABASE_NAME)
    #create_database_child(child1)
    #create_database_child2(child2)
    
    with connect_postgres(dbname=DATABASE_NAME) as conn_project, \
        connect_postgres(dbname=child1) as conn_child1, \
        connect_postgres(dbname=child2) as conn_child2:
        conn_project.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child2.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        #create_table(conn_project)
        #create_table1(conn_child1)
        #create_table1(conn_child2)
        #horizontal_fragmentation_by_region(conn_project, 'Tempe')
        #vertical_fragmentation_likes_shares(conn_project)
        #add_user(conn_project, 'Pujith ', 'Tempe', 'pujithsaip@gmail.com')
        #add_user(conn_project, 'Rishi ', 'Tempe', 'rishikumar@gmail.com')
        #add_user(conn_project, 'Ajay1', 'New York', 'ajay1@yahoo.com')
        #add_post(conn_project, 1, "My post", 122, 1)
        #add_post(conn_project,3,"last one",232,12)
        #add_post(conn_project,2,'light up',233,21)
        #update_post(conn_project, 2, "Updates", 3)
        #update_post(conn_project,1,"No Rules",1)
        #update_user(conn_project, 2, 'Rishi Kumar', 'Tempe', 'rishikumar@gmail.com')
        #update_user(conn_project, 3, 'Ajay', 'NewYork', 'ajay@yahoo.com')
        #delete_post(conn_project, 2, 3)
        #delete_user(conn_project, 1, 'Pujith')
        #print_data_in_child(conn_project)
        #print_data_in_child1(conn_child1)
        #print_data_in_child1(conn_child2)
        #run_explain('select username from userprofile',conn_project)
        #create_index('userprofile','userid',conn_project)

        # Generate user profile data
        #userprofiles_data = generate_random_userprofiles(1000000)

        # Connect to the database and call batch_insert function with the generated data
        #batch_insert(conn_child2, 101, table_name='userprofile', data_list=userprofiles_data, batch_size=10)

        # Example original and optimized queries
        #original_query = """
        #SELECT 
        #  u.userid, 
        #  u.username, 
        #  u.location, 
        #  u.email, 
        #  (SELECT COUNT(*) FROM post p WHERE p.userid = u.userid) as post_count
        #FROM 
        #  userprofile u;
        #"""
        #
        #optimized_query = """
        #SELECT 
        #  u.userid, 
        #  u.username, 
        #  u.location, 
        #  u.email, 
        #  COUNT(p.postid) as post_count
        #FROM 
        #  userprofile u
        #LEFT JOIN 
        #  post p ON u.userid = p.userid
        #GROUP BY 
        #  u.userid;
        #"""
        #
        ## Assume conn is your established database connection
        ## You would call refactor_query like this:
        #refactor_query_test(conn_project, original_query, optimized_query)
        # Connection parameters list
        conn_params_list = [conn_child1,conn_child2]
        # Create indexes on each child database
        #create_index_on_children(conn_params_list, 'userprofile', 'location')
        create_index(conn_child1, 'userprofile', 'location')
        # Simulate a distributed query that fetches user profiles based on location
        
        #query = "SELECT * FROM userprofile WHERE location = 'New York';"
        #results = execute_query_on_children(conn_params_list, query)

        # The query to execute
        
        query = "SELECT * FROM userprofile WHERE location = %s;"
        query_params = ("ndislvc niducsadxknccsakjnddxisjsx",)

        # Measure query performance on all databases before creating the index
        #print("Measuring query performance without indexes...")
        total_time_without_index = execute_query_on_all_dbs(conn_params_list, query, query_params)
        print(f'total_time_without_index is {total_time_without_index}')

        
    
    



