import psycopg2
import psycopg2.extensions
from pyignite import Client
from pyignite.datatypes.prop_codes import *

DATABASE_NAME = 'project'
child1 = 'child1'
child2 = 'child2'

def connect_postgres(dbname):
    """Connect to PostgreSQL using psycopg2 with the specified database name"""
    return psycopg2.connect(
        host="localhost",
        database=dbname,
        user="postgres",
        password="postgres",
    )

def connect_ignite():
    ignite = Client()
    ignite.connect('127.0.0.1', 10800)
    return ignite

# Initialize connections
conn_project = connect_postgres(dbname=DATABASE_NAME)
conn_child1 = connect_postgres(dbname=child1)
conn_child2 = connect_postgres(dbname=child2)
ignite = connect_ignite()
my_cache = ignite.get_or_create_cache({PROP_NAME: 'my_cache'})

def begin_distributed_transaction():
    # Begin distributed transaction
    conn_project.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn_child1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn_child2.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Start Ignite transaction
    ignite_transaction = ignite.tx_start()

    return conn_project, conn_child1, conn_child2, ignite_transaction

def commit_distributed_transaction(ignite_transaction):
    # Commit PostgreSQL transactions
    conn_project.commit()
    conn_child1.commit()
    conn_child2.commit()

    # Commit Ignite transaction
    ignite_transaction.commit()

def rollback_distributed_transaction(ignite_transaction):
    # Rollback PostgreSQL transactions
    conn_project.rollback()
    conn_child1.rollback()
    conn_child2.rollback()

    # Rollback Ignite transaction
    ignite_transaction.rollback()

def add_comment(conn, comment_id, post_id, user_id, content):
    with conn.cursor() as cursor:
        # Acquire a row-level lock on the Comment table
        cursor.execute("SELECT * FROM Comment WHERE FALSE FOR UPDATE")
        
        # Insert the comment
        cursor.execute("INSERT INTO Comment (CommentID, PostID, UserID, Content, Timestamp) VALUES (%s, %s, %s, %s, NOW())",
                       (comment_id, post_id, user_id, content))

        # Store the comment in Ignite for caching
        my_cache.put(comment_id, {'PostID': post_id, 'UserID': user_id, 'Content': content})

def get_comment_from_cache(comment_id):
    # Retrieve the comment from Ignite cache
    result = my_cache.get(comment_id)
    return result

def add_comment_distributed(comment_id, user_id, post_id, content):
    try:
        conn_project, conn_child1, conn_child2, ignite_transaction = begin_distributed_transaction()

        # Retrieve the comment from Ignite cache
        existing_comment = get_comment_from_cache(comment_id)

        if existing_comment:
            # If comment is present in the cache then it has already been added to the database
            pass

        # Add comment in the project database
        add_comment(conn_project, comment_id, user_id, post_id, content)

        # Add comment in child databases
        add_comment(conn_child1, comment_id, user_id, post_id, content)
        add_comment(conn_child2, comment_id, user_id, post_id, content)

        commit_distributed_transaction(ignite_transaction)

    except Exception as e:
        print(f"Error in distributed transaction: {e}")
        rollback_distributed_transaction(ignite_transaction)

# Example usage
add_comment_distributed(17, 1, 1, "test2")