from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import datetime
import pymongo

class Post:
    def __init__(self, userId, postId, postType, text, url, timestamp):
        self.userId = userId
        self.postId = postId
        self.postType = postType
        self.text = text
        self.url = url
        self.timestamp = timestamp

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
print()
print("Cur databases: ")
print(client.list_database_names())
print()
print("Creating Databases")

# Create or get databases
master_db = client["master_db"]
slave1_db = client["slave1_db"]
slave2_db = client["slave2_db"]

# Create or get collections
master_collection = master_db["Posts"]
slave1_collection = slave1_db["Posts"]
slave2_collection = slave2_db["Posts"]
print()
print("Databases Created")
print()

# CRUD operations
def add(collection, post):
    collection.insert_one(post)

def getAll(collection, query=None):
    return list(collection.find()) if query else list(collection.find())

def getPost(collection, postId):
    return collection.find_one({"postId": postId})

def update(collection, postId, post):
    collection.update_one({"postId": postId}, {"$set": post})

def delete(collection, postId):
    collection.delete_one({"postId": postId})

#Inserting in master and slave nodes with replication factor 2
def addPost(postId, userId, type, text, url, timestamp):
    post = Post(postId, userId, type, text, url, timestamp)
    add(master_collection, post.__dict__)
    postId = post.postId
    if(postId%2==0):
        add(slave2_collection, post.__dict__)
    else:
        add(slave1_collection, post.__dict__)

# Distributed Querying
def getAllPosts():
    list1 = getAll(slave1_collection)
    list2 = getAll(slave2_collection)
    return list1.extend(list2)

def getAllPostsByUSer(userId):
    return master_collection.find({"userId": userId})

def updatePost(postId, updatedPost):
    post = getPost(master_collection, postId)
    post["text"] = updatedPost["text"]
    post["timestamp"] = datetime.datetime.now()
    
    update(master_collection, postId, post)
    if(postId%2==0):
        update(slave2_collection, postId, post)
    else:
        update(slave1_collection, postId, post)

def deletePost(postId):
    delete(master_collection, postId)
    if(postId%2==0):
        delete(slave2_collection, postId)
    else:
        delete(slave1_collection, postId)

# Creating indexes for query optimization and performance
master_collection.create_index([("postId", pymongo.ASCENDING)])
slave1_collection.create_index([("postId", pymongo.ASCENDING)])
slave2_collection.create_index([("postId", pymongo.ASCENDING)])

# Creating indexes for query optimization and performance
master_collection.create_index([("userId", pymongo.ASCENDING)])
slave1_collection.create_index([("userId", pymongo.ASCENDING)])
slave2_collection.create_index([("userId", pymongo.ASCENDING)])

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
        print("Inconsistent Database system")
        new_master_db = client[master_db_name]
        for document in client[slave1_db_name].Posts.find({"postId": {"$mod": [2, 1]}}):
            new_master_db.posts.insert_one(document)
        for document in client[slave2_db_name].Posts.find({"postId": {"$mod": [2, 0]}}):
            new_master_db.posts.insert_one(document)
        print("Restored consistency: Created new master_db")

    # Check if slave1_db is alive, If slave1_db is not alive, create a new slave1_db and insert data with odd postId from master_db
    if slave1_db_name not in cur_all_databases:
        print("Inconsistent Database system")
        new_slave1_db = client[slave1_db_name]
        for document in client[master_db_name].Posts.find({"postId": {"$mod": [2, 1]}}):
            new_slave1_db.posts.insert_one(document)
        print("Restored consistency: Created new slave1_db")

    # Check if slave2_db is alive, If slave2_db is not alive, create a new slave2_db and insert data with even postId from master_db
    if slave2_db_name not in cur_all_databases:
        print("Inconsistent Database system")
        new_slave2_db = client[slave2_db_name]
        docs = client[master_db_name].Posts.find({"postId": {"$mod": [2, 0]}})
        for document in docs:
            new_slave2_db.posts.insert_one(document)
        print("Restored consistency: Created new slave2_db")


print("Post Added")
addPost( 8, 9, "Text", "This is post 1", "http://www.posts.com/1", datetime.datetime.now())

print("Master DB Posts:")
print(getAll(master_collection))
print()
print("Slave 1 DB Posts:")
print(getAll(slave1_collection))
print()
print("Slave 2 DB Posts:")
print(getAll(slave2_collection))
print()

print("Updating Post")
updatePost(9, {"text": "Updated content of post 1"})
print("Master DB Posts:")
print(getAll(master_collection))
print()
print("Slave 1 DB Posts:")
print(getAll(slave1_collection))
print()
print("Slave 2 DB Posts:")
print(getAll(slave2_collection))
print()

print("Deleting posts")
deletePost(9)
print("Master DB Posts:")
print(getAll(master_collection))
print()
print("Slave 1 DB Posts:")
print(getAll(slave1_collection))
print()
print("Slave 2 DB Posts:")
print(getAll(slave2_collection))
print()

print("Adding 2 posts")
addPost( 1, 1, "Text", "This is post 1", "http://www.posts.com/1", datetime.datetime.now())
addPost(1, 2, "Text", "This is post 2", "http://www.posts.com/2", datetime.datetime.now())
print()

print("Data distribution between slave nodes")
print("Master DB Posts:")
print(getAll(master_collection))
print()
print("Slave 1 DB Posts:")
print(getAll(slave1_collection))
print()
print("Slave 2 DB Posts:")
print(getAll(slave2_collection))
print()


print("System Consistency Management")
print("\nDeleting Slave 1 DB")
client.drop_database("slave1_db")
print("Cur databases: ")
print(client.list_database_names())
restore_consistency(client)
print("Cur databases: ")
print(client.list_database_names())
print()

print("System Consistency Management")
print("\nDeleting Slave 2 DB")
client.drop_database("slave2_db")
print("Cur databases: ")
print(client.list_database_names())
restore_consistency(client)
print("Cur databases: ")
print(client.list_database_names())
print()

print("System Consistency Management")
print("\nDeleting Master DB")
print("Cur databases: ")
client.drop_database("master_db")
restore_consistency(client)
print("Cur databases: ")
print(client.list_database_names())
print()

# client.drop_database("master_db")
# client.drop_database("slave1_db")
# client.drop_database("slave2_db")