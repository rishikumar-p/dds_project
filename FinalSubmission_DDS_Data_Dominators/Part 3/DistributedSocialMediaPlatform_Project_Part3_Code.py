"""
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

#Optimization Script using EXPLAIN and EXPLAIN ANALYZE
def run_explain(query, conn):
    """Run EXPLAIN and EXPLAIN ANALYZE on a given query and print the results."""
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
   
    with connect_postgres(dbname=DATABASE_NAME) as conn_project, \
        connect_postgres(dbname=child1) as conn_child1, \
        connect_postgres(dbname=child2) as conn_child2:
        conn_project.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn_child2.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        
        #run_explain('select username from userprofile',conn_project)
        create_index('userprofile','userid',conn_project)

        # Generate user profile data
        userprofiles_data = generate_random_userprofiles(1000000)

        # Connect to the database and call batch_insert function with the generated data
        batch_insert(conn_child2, 101, table_name='userprofile', data_list=userprofiles_data, batch_size=10)

        # Example original and optimized queries
        original_query = """
        SELECT 
          u.userid, 
          u.username, 
          u.location, 
          u.email, 
          (SELECT COUNT(*) FROM post p WHERE p.userid = u.userid) as post_count
        FROM 
          userprofile u;
        """
        
        optimized_query = """
        SELECT 
          u.userid, 
          u.username, 
          u.location, 
          u.email, 
          COUNT(p.postid) as post_count
        FROM 
          userprofile u
        LEFT JOIN 
          post p ON u.userid = p.userid
        GROUP BY 
          u.userid, u.username, u.location, u.email;
        """
        
        # Assume conn is your established database connection
        # You would call refactor_query like this:
        #refactor_query(conn_child2, original_query, optimized_query)
        # Connection parameters list
        conn_params_list = [conn_child2]
        # Create indexes on each child database        
        create_index(conn_child1, 'userprofile', 'location')
        create_index(conn_child2, 'userprofile', 'location')
        
        # Simulate a distributed query that fetches user profiles based on location        
        query = "SELECT * FROM userprofile WHERE location = %s;"
        query_params = ("ndislvc niducdssakjdnasolnkfkdsl vckisndfisdnflkdasnkjfnsadxknccsakjnddxisjsx",)

        # Measure query performance on all databases before creating the index
        #print("Measuring query performance without indexes...")
        total_time_without_index = execute_query_on_all_dbs(conn_params_list, query, query_params)
        print(f'total_time_without_index is {total_time_without_index}')

        create_index(conn_child2, 'userprofile', 'location')

        total_time_without_index = execute_query_on_all_dbs(conn_params_list, query, query_params)
        print(f'total_time_with_index is {total_time_without_index}')

        
    
    



