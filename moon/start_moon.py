import os
from dotenv import load_dotenv
from communication.communication import Communication

load_dotenv()

comm = Communication(os.environ['MOON_HOST'], int(os.environ['MOON_PORT']))
