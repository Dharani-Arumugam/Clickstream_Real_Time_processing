import argparse
import random
import uuid
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import json

import event_tracker
from producer_config import *

TOPICS = ['impressions','clicks', 'conversions']
KAFKA_BROKERS = 'localhost:29092,localhost:39092,localhost:49092'
NUM_PARTITIONS = 24
REPLICATION_FACTOR = 1
LOCATIONS =['US','IN','UK','CA','AU','EU']


logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ----------------------------------------------------
# Creating Topics for impressions, clicks, conversion
# ---------------------------------------------------
def create_topic(kafka_topics):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})

    #Building NewTopic objects
    new_topics =[NewTopic(
        topic=topic,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR) for topic in kafka_topics]

    # creating the NewTopics (Kafka will throw error if already exists
    fs = admin_client.create_topics(new_topics)

    for topic, futures in fs.items():
        try:
            futures.result()
            logger.info(f'Topic {topic} created successfully')
        except Exception as e:
            error_msg = str(e)
            if "TOPIC_ALREADY_EXISTS" in error_msg or 'TopicExistsError' in error_msg:
                logger.info(f'Topic {topic} already exists')
            else:
                logger.error(f'Failed to create topic {topic}: {e}')
# --------------------------------
# Generating impression events
# -------------------------------
def generate_impression_event(time_now):
    user_id = random.randint(1, NUM_OF_USERS)
    campaign_id = random.randint(1, NUM_OF_CAMPAIGN)
    ad_id = (campaign_id * ADS_PER_CAMPAIGN) + random.randint(1, ADS_PER_CAMPAIGN)
    join_key = stable_join_key(user_id, campaign_id,ad_id)
    return {
        "event_type": "impression",
        "event_id":str(uuid.uuid4()),
        "event_timestamp": iso_timestamp_to_str(time_now),
        "event_time_epoch_ms": int(time_now * 1000),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "ad_id": ad_id,
        "join_key": join_key,
        "location":random.choice(LOCATIONS),
        "devices":random.choice(['ios','android','web','windows','linux','mac']),
        "cost_micros":random.randint(50,5000)
    }

# --------------------------------
# Generating click events
# -------------------------------
def generate_click_event(time_now, funnel_mem):
    impr_base = funnel_mem.sample_impression()
    if impr_base:
        click_time = (impr_base['event_time_epoch_ms']/1000.0) + random.randint(1, CLICK_WINDOW)
        user_id = impr_base['user_id']
        campaign_id = impr_base['campaign_id']
        ad_id = impr_base['ad_id']
        join_key = impr_base['join_key']
        impression_event_id = impr_base['event_id']
    else:
        user_id = random.randint(1, NUM_OF_USERS)
        campaign_id = random.randint(1, NUM_OF_CAMPAIGN)
        ad_id = (campaign_id * ADS_PER_CAMPAIGN) + random.randint(1, ADS_PER_CAMPAIGN)
        join_key = stable_join_key(user_id, campaign_id,ad_id)
        click_time = time_now
        impression_event_id = None

    return {
        "event_type": "click",
        "event_id":str(uuid.uuid4()),
        "event_timestamp": iso_timestamp_to_str(click_time),
        "event_time_epoch_ms": int(click_time * 1000),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "ad_id": ad_id,
        "join_key": join_key,
        "impression_event_id": impression_event_id,
        "click_per_cost_micros": random.randint(100,20000)
    }
# --------------------------------
# Generating conversion events
# -------------------------------
def generate_conversion_event(time_now, funnel_mem):
    click_base = funnel_mem.sample_click()
    if click_base:
        conversion_time = (click_base['event_time_epoch_ms']/1000.0) + random.randint(1, CONVERSION_WINDOW)
        user_id = click_base['user_id']
        campaign_id = click_base['campaign_id']
        ad_id = click_base['ad_id']
        join_key = click_base['join_key']
        click_event_id = click_base['event_id']
    else:
        user_id = random.randint(1, NUM_OF_USERS)
        campaign_id = random.randint(1, NUM_OF_CAMPAIGN)
        ad_id = (campaign_id * ADS_PER_CAMPAIGN) + random.randint(1, ADS_PER_CAMPAIGN)
        join_key = stable_join_key(user_id, campaign_id,ad_id)
        conversion_time = time_now
        click_event_id = None
    return {
        "event_type": "conversion",
        "event_id":str(uuid.uuid4()),
        "event_timestamp": iso_timestamp_to_str(conversion_time),
        "event_time_epoch_ms": int(conversion_time * 1000),
        "user_id": user_id,
        "campaign_id": campaign_id,
        "ad_id": ad_id,
        "join_key": join_key,
        "click_event_id": click_event_id,
        "purchase_value": round(random.uniform(5,1000),2),
        "currency":"USD"
    }

def main():
    # arg parser
    parse = argparse.ArgumentParser()
    parse.add_argument('--bootstrap_servers', type=str,  help="Kafka bootstrap servers",
                       default=KAFKA_BROKERS)
    parse.add_argument('--events_per_sec', type=int, default=EVENTS_PER_SEC,
                       help="Number of events per second across all topics")
    parse.add_argument('--impr_ratio', type=float, default=IMPRESSION_RATIO, help="Impression ratio")
    parse.add_argument('--click_ratio', type=float, default=CLICK_RATIO, help="Click ratio")
    parse.add_argument('--conversion_ratio', type=float, default=CONVERSION_RATIO, help="Conversion ratio")
    args = parse.parse_args()


    # Validate if ratio's equals to 1
    if abs(args.impr_ratio + args.click_ratio + args.conversion_ratio - 1.0) > 1e-6:
        raise ValueError('Ratios must sum up to 1.0')

    # create the topics in kafka
    create_topic(TOPICS)

    #Producer config
    producer = Producer({
        'bootstrap.servers': args.bootstrap_servers,
        'queue.buffering.max.messages': 1_000_000,
        'compression.type': 'lz4',
        'linger.ms':20,
        'batch.size': 131072,
        'acks': 1,
    })
    #Assign a in-memory to store a cache of events
    funnel_mem = event_tracker.EventTracker()
    produced_events_cnt = 0
    time_start = time.time()
    last_time_log = time_start
    try:
        while True:
            tick_start = time.time()
            time_now = tick_start

            for _ in range(args.events_per_sec):
                r = random.random()
                if r < args.impr_ratio:
                    impression_event = generate_impression_event(time_now)
                    impression_event = generating_late_data(impression_event)
                    impression_event, duplicate_event = generating_duplicate_data(impression_event)
                    #add to EventTracker
                    funnel_mem.add_impression(impression_event)

                    key = str(impression_event['join_key']).encode('utf-8')
                    producer.produce(TOPICS[0], key=key, value=json.dumps(impression_event).encode('utf-8'), callback=message_delivery)
                    produced_events_cnt += 1
                    # if duplicates
                    if duplicate_event:
                        producer.produce(TOPICS[0], key=key, value=json.dumps(duplicate_event).encode('utf-8'), callback=message_delivery)
                        produced_events_cnt += 1
                elif r < args.impr_ratio + args.click_ratio:
                    click_event = generate_click_event(time_now, funnel_mem)
                    click_event = generating_late_data(click_event)
                    click_event, duplicate_event = generating_duplicate_data(click_event)

                    #add to EventTracker
                    funnel_mem.add_click(click_event)
                    key = str(click_event['join_key']).encode('utf-8')
                    producer.produce(TOPICS[1], key=key, value=json.dumps(click_event).encode('utf-8'), callback=message_delivery)
                    produced_events_cnt += 1
                    if duplicate_event:
                        producer.produce(TOPICS[1],key=key, value=json.dumps(duplicate_event).encode('utf-8'), callback=message_delivery)
                        produced_events_cnt += 1

                else:
                    conversion_event = generate_conversion_event(time_now, funnel_mem)
                    conversion_event = generating_late_data(conversion_event)
                    conversion_event, duplicate_event = generating_duplicate_data(conversion_event)

                    key = str(conversion_event['join_key']).encode('utf-8')
                    producer.produce(TOPICS[2], key=key, value=json.dumps(conversion_event).encode('utf-8'), callback=message_delivery)
                    produced_events_cnt += 1
                    if duplicate_event:
                        producer.produce(TOPICS[2], key=key, value=json.dumps(duplicate_event).encode('utf-8'), callback=message_delivery)
                        produced_events_cnt += 1
                producer.poll(0)

            # throttle to ~1 second per tick
            elapsed = time.time() - tick_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

            if time.time() - last_time_log >= 5:
                dt = time.time() - time_start
                real_throughput = produced_events_cnt / dt if dt > 0 else 0
                print(f"Produced {produced_events_cnt:,} events | avg {real_throughput:,.0f} events/sec")
                last_time_log = time.time()
    finally:
        producer.flush(10)

if __name__ == "__main__":
    main()