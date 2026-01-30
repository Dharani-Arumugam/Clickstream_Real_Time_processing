import hashlib
import time
import random


EVENTS_PER_SEC = 20000
IMPRESSION_RATIO = 0.80
CLICK_RATIO = 0.18
CONVERSION_RATIO = 0.02

#  variables used for event generation
NUM_OF_USERS = 3_000_000
NUM_OF_CAMPAIGN = 500
ADS_PER_CAMPAIGN = 100
CLICK_WINDOW = 30 * 60 # Half an hour window between impression and click
CONVERSION_WINDOW = 24 * 60 * 60 # 24 hour window between click and conversion

# variables for late arriving data and duplicate data
LATE_PERCENTAGE = 0.05
LATE_WINDOW_MIN = 10 * 60 #10 mins
LATE_WINDOW_MAX = 90 * 60 #90 mins
DUPLICATE_PERCENTAGE = 0.02
# Helper functions
# -------------------------------------------------------
# creating a stable numeric join key for kafka partition
# -------------------------------------------------------
def stable_join_key(user_id, campaign_id, ad_id):
    raw = f'{user_id}|{campaign_id}|{ad_id}'.encode('utf-8')
    digest = hashlib.sha256(raw).digest()
    return int.from_bytes(digest[:8],byteorder='big', signed=False)


# -----------------------------------------
# Converting epoch time in sec to string
# -----------------------------------------
def iso_timestamp_to_str(epoch_sec):
    millisec = int(epoch_sec * 1000)
    seconds = millisec // 1000
    remaining_millisec = millisec % 1000
    return time.strftime('%Y-%m-%dT%H:%M:%S',time.gmtime(seconds)) + f'.{remaining_millisec:03d}Z'

#-----------------------------------------
# Message Delivery callback function
# -----------------------------------------
def message_delivery(err, msg):
    if err is not None:
        print(f'Delivery failed with error: {err}')


#-----------------------------------------
# Generating late arriving data
# -----------------------------------------
def generating_late_data(event):
    if random.random() >= LATE_PERCENTAGE:
        return event
    late_by_sec = random.randint(LATE_WINDOW_MIN, LATE_WINDOW_MAX)
    late_by_time = (event['event_time_epoch_ms']/1000) - late_by_sec
    new_event = dict(event)
    new_event['event_time'] = iso_timestamp_to_str(late_by_time)
    new_event['event_time_epoch_ms'] = int(late_by_time * 1000)
    new_event['is_late'] = True
    new_event['late_by_sec'] = late_by_sec
    return new_event
#-----------------------------------------
# Generating duplicate data
# -----------------------------------------
def generating_duplicate_data(event):
    if random.random() >= DUPLICATE_PERCENTAGE:
        return event, None
    duplicate_event = dict(event)
    duplicate_event['is_duplicate'] = True
    return event, duplicate_event
