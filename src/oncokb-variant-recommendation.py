import os

from dotenv import load_dotenv

from utils import inc

load_dotenv()
test = os.environ.get("TEST")
print(test)

print(inc(1))
