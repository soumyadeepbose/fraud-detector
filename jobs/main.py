from datetime import timedelta
from dateutil import parser
from confluent_kafka import SerializingProducer
import os, json, random, uuid, time

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
FRAUD_TOPIC = os.getenv('FRAUD_TOPIC', 'fraud_test_topic_1')
start_time = timedelta(hours=0, minutes=0, seconds=0)

def get_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_transaction_data(company_id):
    return {
        'unique_id': uuid.uuid4(),
        'company_id': company_id,
        'transaction_id': str(hex(random.randint(50000, 100000))),
        'transaction_time': parser.parse(str(get_time())).isoformat(),
        'transaction_amount': random.randint(1, 1000)
        
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable.')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key=str(data['unique_id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()
    # print(f'Message produced: {data}')

def simulate_data(producer, company_id):
    while True:
        company_transaction_data = simulate_transaction_data(company_id)
        
        produce_to_kafka(producer, FRAUD_TOPIC, company_transaction_data)

        time.sleep(5)
        


if __name__ == '__main__':
    producer_config = SerializingProducer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Error: {err}')
    })

    try:
        simulate_data(producer_config, 'company-123') # company id
    except KeyboardInterrupt:
        print('Interrupted by the user.')
    except Exception as e:
        print(f'Following error occurred: {e}')
        start_time = timedelta(hours=0, minutes=0, seconds=0)
        pass