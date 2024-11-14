import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "kylin://admin:password@kylin/kylin")
    KYLIN_API_URL = os.getenv("KYLIN_API_URL", "http://kylin:7070/kylin/api")
    KYLIN_USER = os.getenv("KYLIN_USER", "ADMIN")
    KYLIN_PASSWORD = os.getenv("KYLIN_PASSWORD", "KYLIN")
