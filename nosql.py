from pymongo import MongoClient
from Post import Post
import datetime
import pymongo

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create or get databases
master_db = client["master_db"]
slave1_db = client["slave1_db"]
slave2_db = client["slave2_db"]

# Create or get collections
master_collection = master_db["Posts"]
slave1_collection = slave1_db["Posts"]
slave2_collection = slave2_db["Posts"]

# # Sharding for horizontal fragmentation
# master_db.command("shardCollection", "master_db.Posts", key={"postType": 1})
# slave1_db.command("shardCollection", "slave1_db.Posts", key={"postType": 1})
# slave2_db.command("shardCollection", "slave2_db.Posts", key={"postType": 1})


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

# creating indexes
master_collection.create_index([("postId", pymongo.ASCENDING)])
slave1_collection.create_index([("postId", pymongo.ASCENDING)])
slave2_collection.create_index([("postId", pymongo.ASCENDING)])

