import os
import sys
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# Settings
page_size = 1000  # Number of users to process by page. If there're too many messages per user, may need to decrease this value.
delete_before_days = 365  # Oldest message to keep in days.
delete_if_more_than = 1000  # Max messages per user.

# Load Database
cred = credentials.Certificate(os.getenv('FIREBASE_ADMIN_SDK_JSON_FILE_PATH'))

firebase_admin.initialize_app(cred, {
    'databaseURL': os.getenv('FIREBASE_DB_URL'),
})

ref = db.reference()

# Process Messages
delete_before_date = datetime.now(timezone.utc) - timedelta(days=delete_before_days)


def process_page(snapshot_first_key=None):
    try:
        if snapshot_first_key:
            snapshot = ref.order_by_key().start_at(snapshot_first_key).limit_to_first(page_size).get()
        else:
            snapshot = ref.order_by_key().limit_to_first(page_size).get()

        if snapshot:
            # Check Page
            snapshot_updates = {}
            for user_id, user_messages in snapshot.items():
                has_messages = False
                count_messages = 0
                for message_id, message in reversed(list(user_messages.items())):
                    if count_messages >= delete_if_more_than:
                        print(f'Will delete message {user_id}/{message_id} - Too many')
                        snapshot_updates[f'{user_id}/{message_id}'] = None
                    elif not message.get('created_at') or (
                            datetime.fromisoformat(message['created_at']) <= delete_before_date):
                        print(f'Will delete message {user_id}/{message_id} - Too old')
                        snapshot_updates[f'{user_id}/{message_id}'] = None
                    else:
                        count_messages += 1
                    has_messages = True
                if not has_messages:
                    print(f'Will delete user ${user_id} - No messages')
                    snapshot_updates[user_id] = None
            # Update Page
            if snapshot_updates:
                ref.update(snapshot_updates)
            # Next Page
            snapshot_last_key = list(snapshot.keys())[-1]
            if snapshot_last_key != snapshot_first_key:
                process_page(snapshot_last_key)
            else:
                print('Completed!')
        else:
            print('Completed!')
    except Exception as e:
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


if __name__ == '__main__':
    process_page()
