import os
import sys

import firebase_admin
from firebase_admin import credentials, db
import datetime
import pytz

cred = credentials.Certificate('<path to the adminsdk cred json file>')
firebase_admin.initialize_app(cred, {
    'databaseURL': '<database-url>'
})

page_size = 1000
delete_before_months = 6
delete_if_more_than = 1000

database = db.reference()

utc_current_date = datetime.datetime.now(pytz.UTC) - datetime.timedelta(days=delete_before_months * 30)


def process_page(snapshot_first_key=None):
    try:

        print(f"Starting from {snapshot_first_key or 'beginning'}")
        page_query = None
        if snapshot_first_key:
            page_query = database.order_by_key().start_at(snapshot_first_key).limit_to_first(page_size)
        else:
            page_query = database.order_by_key().limit_to_first(page_size)

        snapshot = page_query.get()
        if snapshot:
            # Check Page
            snapshot_updates = {}
            for user_key, user in snapshot.items():
                has_messages = False
                count_messages = 0
                for message_id, message in sorted(user.items(), reverse=True):
                    created_at_utc = datetime.datetime.fromisoformat(message['created_at']).astimezone(pytz.UTC)  \
                        if message.get('created_at') else None
                    if count_messages >= delete_if_more_than:
                        print(f"delete message {user_key}/{message_id} - Too many")
                        snapshot_updates[f"{user_key}/{message_id}"] = {}
                    elif created_at_utc and created_at_utc <= utc_current_date:
                        print(f"delete message {user_key}/{message_id} - Too old")
                        snapshot_updates[f"{user_key}/{message_id}"] = {}
                    else:
                        count_messages += 1
                    has_messages = True
                if not has_messages:
                    print(f"delete user {user_key} - No messages")
                    snapshot_updates[user_key] = {}

            # Update Page
            database.update(snapshot_updates)

            # Next Page
            snapshot_last_key = list(snapshot.keys())[-1]
            if snapshot_last_key != snapshot_first_key:
                process_page(snapshot_last_key)
            else:
                print("Completed!")
        else:
            print("Completed!")
    except Exception as e:
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


if __name__ == '__main__':
    process_page()
