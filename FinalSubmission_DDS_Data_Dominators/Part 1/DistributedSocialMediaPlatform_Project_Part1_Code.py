# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 20:09:55 2023

@author: ppanchum
"""

import psycopg2
import psycopg2.extensions
import hashlib

DATABASE_NAME = 'project'
child1 = 'child1'
child2 = 'child2'


def create_database(dbname):
    """Connect to PostgreSQL and create a database named {DATABASE_NAME}"""
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="pass",
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
        password="pass",
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
        password="pass",
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
        password="pass",
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
            -- Other post attributes
            FOREIGN KEY (UserID) REFERENCES UserProfile(UserID)
        )
    ''')
    cursor.close()    

def print_data_in_child(conn_child1):
    table_names = ['UserProfile', 'Friendship','Post', 'Comment']

    #,'userprofile_tempe','PostLikesShares'    

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
        password="pass",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="pass",
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
    
    


def add_post(conn, user_id):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO Post (UserID) VALUES (%s) RETURNING PostID", (user_id,))
        post_id = cursor.fetchone()[0]

    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer

    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child2.autocommit = True

    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor_child1:
            cursor_child1.execute("INSERT INTO Post (PostID, UserID) VALUES (%s, %s)", (post_id, user_id))
    else:
        with conn_child2.cursor() as cursor_child2:
            cursor_child2.execute("INSERT INTO Post (PostID, UserID) VALUES (%s, %s)", (post_id, user_id))
     

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
        password="pass",
        port=5432
    )
    conn_child2.autocommit = True

    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor_child1:
            # Delete posts of the user (and associated comments)
            cursor_child1.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
            # Delete the user
            cursor_child1.execute("DELETE FROM UserProfile WHERE UserID = %s", (user_id,))
    else:
        with conn_child2.cursor() as cursor_child2:
            # Delete posts of the user (and associated comments)
            cursor_child2.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
            # Delete the user
            cursor_child2.execute("DELETE FROM UserProfile WHERE UserID = %s", (user_id,))

        
    
     
        
def delete_post(conn, post_id, user_id):
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
        password="pass",
        port=5432
    )
    conn_child2.autocommit = True

    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor_child1:
            cursor_child1.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))
    else:
        with conn_child2.cursor() as cursor_child2:
            cursor_child2.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))

    

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
            password="pass",
            port=5432
        )
        conn_child2.autocommit = True

        
        conn_child1 = psycopg2.connect(
            host="localhost",
            database="child1",
            user="postgres",
            password="pass",
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
     

def update_post(conn, post_id, user_id):
    with conn.cursor() as cursor:
        cursor.execute("UPDATE Post SET UserID = %s WHERE PostID = %s", (user_id, post_id))
    cursor.close()
    
    
    hash_value = hashlib.sha256(str(user_id).encode()).hexdigest()
    hash_integer = int(hash_value, 16)  # Convert hexadecimal hash to integer
    
    conn_child2 = psycopg2.connect(
        host="localhost",
        database="child2",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child2.autocommit = True

    
    conn_child1 = psycopg2.connect(
        host="localhost",
        database="child1",
        user="postgres",
        password="pass",
        port=5432
    )
    conn_child1.autocommit = True

    if hash_integer % 2 == 0:
        with conn_child1.cursor() as cursor:
            cursor.execute("UPDATE Post SET UserID = %s WHERE PostID = %s", (user_id, post_id))
    else:
        
        with conn_child2.cursor() as cursor:
            cursor.execute("UPDATE Post SET UserID = %s WHERE PostID = %s", (user_id, post_id))
    
    

def update_comment(conn, comment_id, new_content):
    
    
    with conn.cursor() as cursor:
        cursor.execute("UPDATE Comment SET Content = %s WHERE CommentID = %s", (new_content, comment_id))
        

    
        
def get_user_by_username(conn, username):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM UserProfile WHERE UserName = %s", (username,))
        result = cursor.fetchone()
        print("get_user_by_username result:", result)
        return result

def get_all_users(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM UserProfile")
        result = cursor.fetchall()
        print("get_all_users result:", result)
        return result

def get_friends_of_user(conn, username):
    with conn.cursor() as cursor:
        cursor.execute("SELECT UserProfile.* FROM UserProfile "
                       "JOIN Friendship ON (UserProfile.UserID = Friendship.UserID1 OR UserProfile.UserID = Friendship.UserID2) "
                       "WHERE UserProfile.UserName = %s", (username,))
        result = cursor.fetchall()
        print("get_friends_of_user result:", result)
        return result

def get_post_by_id(conn, post_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Post WHERE PostID = %s", (post_id,))
        result = cursor.fetchone()
        print("get_post_by_id result:", result)
        return result

def get_all_posts_of_user(conn, user_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Post WHERE UserID = %s", (user_id,))
        result = cursor.fetchall()
        print("get_all_posts_of_user result:", result)
        return result

def get_comment_by_id(conn, comment_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Comment WHERE CommentID = %s", (comment_id,))
        result = cursor.fetchone()
        print("get_comment_by_id result:", result)
        return result

def get_all_comments_on_post(conn, post_id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Comment WHERE PostID = %s", (post_id,))
        result = cursor.fetchall()
        print("get_all_comments_on_post result:", result)
        return result


    

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

# =============================================================================
#         add_user(conn_project, 'Pujith ', 'Tempe', 'pujithsaip@gmail.com')
#         add_user(conn_project, 'Rishi ', 'Tempe', 'rishikumar@gmail.com')
#         add_user(conn_project, 'Ajay', 'Chandler', 'ajay@yahoo.com')
#         add_post(conn_project,3)
#         add_post(conn_project,2)
#         add_friendship(conn_project, 1, 2)
#         add_comment(conn_project, 1, 2, "Good")
#         print_data_in_child(conn_project)
# =============================================================================
# =============================================================================
#         get_all_comments_on_post(conn_project, 1)
#         get_all_posts_of_user(conn_project, 2)
#         get_all_users(conn_project)
#         get_comment_by_id(conn_project, 1)
#         get_post_by_id(conn_project, 1)
#         get_user_by_username(conn_project, "Pujith")
# =============================================================================
        update_user(conn_project, 3, "Ajay", "New York", 'ajay@gmail.com')
        delete_user(conn_project, 1, "Pujith")
        print_data_in_child(conn_project)
        
        
    
    



