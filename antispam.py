import time
from collections import defaultdict

MESSAGE_LIMIT = 5
TIME_WINDOW = 4

user_messages = defaultdict(list)

def is_spam(user_id):

    now = time.time()

    msgs = user_messages[user_id]

    msgs.append(now)

    user_messages[user_id] = [t for t in msgs if now - t < TIME_WINDOW]

    if len(user_messages[user_id]) > MESSAGE_LIMIT:
        return True

    return False