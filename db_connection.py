import os

from dotenv import load_dotenv
from motor import motor_asyncio

load_dotenv()


class MongoConnection:
    def __init__(self):
        self.client = motor_asyncio.AsyncIOMotorClient(f'mongodb://{os.getenv("MONGO_HOST")}:{os.getenv("MONGO_PORT")}')
        self.db = self.client[os.getenv('DB_NAME')]
        self.collection = self.db.get_collection(os.getenv('DB_COLLECTION'))


mongodb = MongoConnection()
