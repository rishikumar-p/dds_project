class Post:
    def __init__(self, userId, postId, postType, text, url, timestamp):
        self.userId = userId
        self.postId = postId
        self.postType = postType
        self.text = text
        self.url = url
        self.timestamp = timestamp