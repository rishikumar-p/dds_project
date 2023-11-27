import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Fault tolerant, Check if all expected databases are present in system
def checkReplicaSetConsistency(client):
    cur_all_databases = client.list_database_names()
    expected_databases = ["master_db", "slave1_db", "slave2_db"]
    print(cur_all_databases)
    return all(db in cur_all_databases for db in expected_databases)

def restore_consistency(client):
    master_db_name = "master_db"
    slave1_db_name = "slave1_db"
    slave2_db_name = "slave2_db"
    cur_all_databases = client.list_database_names()
    # Check if master_db is alive, If master_db is not alive, create a new master_db and insert data from both slave databases
    if master_db_name not in cur_all_databases:
        print("Master")
        new_master_db = client[master_db_name]

        for document in client[slave1_db_name].Posts.find({"postId": {"$mod": [2, 1]}}):
            new_master_db.posts.insert_one(document)
        for document in client[slave2_db_name].Posts.find({"postId": {"$mod": [2, 0]}}):
            new_master_db.posts.insert_one(document)

        print("Restored consistency: Created new master_db")

    # Check if slave1_db is alive, If slave1_db is not alive, create a new slave1_db and insert data with odd postId from master_db
    if slave1_db_name not in cur_all_databases:
        print("Slave 1")
        new_slave1_db = client[slave1_db_name]

        for document in client[master_db_name].Posts.find({"postId": {"$mod": [2, 1]}}):
            new_slave1_db.posts.insert_one(document)

        print("Restored consistency: Created new slave1_db")

    # Check if slave2_db is alive, If slave2_db is not alive, create a new slave2_db and insert data with even postId from master_db
    if slave2_db_name not in cur_all_databases:
        print("Slave 2")
        new_slave2_db = client[slave2_db_name]
        docs = client[master_db_name].Posts.find({"postId": {"$mod": [2, 0]}})
        for document in docs:
            new_slave2_db.posts.insert_one(document)

        print("Restored consistency: Created new slave2_db")


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

print(checkReplicaSetConsistency(client))
restore_consistency(client)
print(checkReplicaSetConsistency(client))