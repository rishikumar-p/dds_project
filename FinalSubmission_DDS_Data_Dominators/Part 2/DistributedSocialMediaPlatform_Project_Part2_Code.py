
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




def connect_postgres(dbname):
    """Connect to PostgreSQL using psycopg2 with the specified database name"""
    return psycopg2.connect(
        host="localhost",
        database=dbname,
        user="postgres",
        password="pass",
        port=5432
    )

def print_data_in_child_horizontal(conn_child1):
    table_names = ['UserProfile', 'Friendship','Post', 'Comment','userprofile_tempe']

    #,'UserProfile_EmailOnly'    

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

def print_data_in_child_vertical(conn_child1):
    table_names = ['UserProfile', 'Friendship','Post', 'Comment','userprofile_tempe','UserProfile_EmailOnly' ]


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

def print_data_in_child(conn_child1):
    table_names = ['UserProfile', 'Friendship','Post', 'Comment']

    #,'UserProfile_EmailOnly'    

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
    print('Child Database')
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
 
 
               
def add_user_crud_vertical(conn, username, location, email):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO UserProfile (UserName, Location) VALUES (%s, %s) RETURNING UserID",
                       (username, location))
        
        user_id = cursor.fetchone()[0]
        cursor.execute("INSERT INTO UserProfile_EmailOnly (UserID,Email) VALUES (%s,%s)",
                       (user_id, email))

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
     
def delete_user_crud_vertical(conn, user_id, user_name):
    with conn.cursor() as cursor:
        # Delete friendships involving the user
        cursor.execute("DELETE FROM Friendship WHERE UserID1 = %s OR UserID2 = %s", (user_id, user_id))
        # Delete comments of the user
        cursor.execute("DELETE FROM Comment WHERE UserID = %s", (user_id,))
        # Delete posts of the user (and associated comments)
        cursor.execute("DELETE FROM Post WHERE UserID = %s", (user_id,))
        
        cursor.execute("DELETE FROM UserProfile_EmailOnly WHERE UserID = %s",(user_id,))
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
            cursor.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))
    else:
        with conn_child2.cursor() as cursor:
            cursor.execute("DELETE FROM Post WHERE PostID = %s", (post_id,))
    
def update_user_crud(conn, user_id, new_username, new_location, new_email):
    with conn.cursor() as cursor:
        cursor.execute("UPDATE UserProfile SET UserName = %s, Location = %s WHERE UserID = %s",
                       (new_username, new_location, user_id))
        cursor.execute("UPDATE UserProfile_EmailOnly SET Email = %s WHERE UserID = %s",
                                      (new_email, user_id))
        
        
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
    
def vertical_fragmentation_email_only(conn):
    """Perform vertical fragmentation by creating a new table for the Email column in the UserProfile table"""

    with conn.cursor() as cursor:

            # Drop the table if it already exists
            cursor.execute('DROP TABLE IF EXISTS UserProfile_EmailOnly')

            # Create a new table for the Email column
            cursor.execute('''
                CREATE TABLE UserProfile_EmailOnly AS
                SELECT UserID, Email FROM UserProfile
            ''')

            # Drop the Email column from the original UserProfile table
            cursor.execute('''
                ALTER TABLE UserProfile
                DROP COLUMN IF EXISTS Email
            ''')

            # Commit the changes
            conn.commit()
            cursor.close()
    

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    
    with connect_postgres(dbname=DATABASE_NAME) as conn_project, \
        connect_postgres(dbname=child1) as conn_child1, \
        connect_postgres(dbname=child2) as conn_child2:
        conn_project.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child2.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)


# =============================================================================
#         horizontal_fragmentation_by_region(conn_project, 'Tempe')
#         print_data_in_child_horizontal(conn_project)
#         vertical_fragmentation_email_only(conn_project)
#         print_data_in_child_vertical(conn_project)
#         update_user_crud(conn_project, 3, "Ajay", 'NewYork', 'ajayreddy@gmail.com')
#         print_data_in_child_vertical(conn_project)
#         print_data_in_child_vertical(conn_project)
# =============================================================================
        print_data_in_child1(conn_child1)
        print_data_in_child1(conn_child2)