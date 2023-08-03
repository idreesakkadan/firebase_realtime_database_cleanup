import time
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime, timedelta, timezone
from termcolor import colored

start = time.time()
# utc_timezone = pytz.timezone("UTC")
current_datetime = datetime.now(timezone.utc)
output_file = open("output_text_files/deleted_nodes-PROD-2.txt", "w")

# Initialize the Firebase Admin SDK with your service account key
cred = credentials.Certificate("<path to the adminsdk cred json file>")
firebase_admin.initialize_app(cred, {
    'databaseURL': '<database-url>'
})

# Get a reference to the specific node
node_ref = db.reference()


# Query all data from the specific node
# data = node_ref.get()

def is_older_than_one_year(created_at_str):
    created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    one_year_ago = current_datetime - timedelta(days=365)
    return created_at < one_year_ago


page_size = 100
start_key = None
num = 1

while True:
    # Query data from the node with pagination
    query = node_ref.order_by_key()
    if start_key:
        query = query.start_at(start_key)

    data = query.limit_to_first(page_size).get()

    # Check if there is any data returned
    if not data:
        break  # Exit the loop if there is no more data

    for key, item in data.items():
        for inner_key, inner_item in item.items():
            if "created_at" in inner_item and is_older_than_one_year(inner_item["created_at"]):
                node_ref.child(key).child(inner_key).delete()
                output_file.write(f"Deleted Record No: {num} {inner_key}\n")
                output_file.write(f"Parent Key: {key}\n\n")
                print(f'Deleted Record{inner_key}')
                num += 1

    start_key = list(data.keys())[-1]

output_file.close()
print(colored("FINISHED", "green"))
print(f'Elapsed time: {time.time() - start} s')
