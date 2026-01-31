import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
USERS_FILE = BASE_DIR / "users.json"

def load_users():
    with open('./users.json', 'r') as f:
        j = json.load(f)
    return j



def verify_user(username, password):
    users = load_users()  # list of dicts

    for user in users:
        if (
            user.get("username") == username
            and user.get("password") == password
        ):
            return True

    return False

