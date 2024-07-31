import streamlit as st
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time, pickle, os, random, uuid, json
from datetime import timedelta, datetime
from dateutil import parser
from confluent_kafka import SerializingProducer, KafkaException
from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
FRAUD_TOPIC = os.getenv('DEAFULT_FRAUD_TOPIC')
start_time = timedelta(hours=0, minutes=0, seconds=0)

def get_time():
    # global start_time
    # start_time = datetime.time.isoformat() # += timedelta(seconds=random.randint(30, 60))
    return datetime.now().time().isoformat()

def get_date():
    return datetime.now().date().isoformat()

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable.')

def delivery_report(err, msg):
    if err is not None:
        st.sidebar.write(f'Message delivery failed: {err}')
    else:
        pass
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# def produce_to_kafka(producer, topic, data):
#     producer.produce(
#         topic=topic,
#         key=str(data['unique_id']),
#         value=json.dumps(data, default=json_serializer).encode('utf-8'),
#         on_delivery=delivery_report
#     )

#     producer.flush()

def producer_error_callback(err):
    if err.code() == KafkaException:
        st.error("Server offline. Please contact the administrator.")

# Initialize Kafka Producer
producer = SerializingProducer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'error_cb': producer_error_callback
})

if "load_state" not in st.session_state:
    st.session_state.load_state = False

if "user_name" not in st.session_state:
    st.session_state["user_name"] = ""
if "company_id" not in st.session_state:
    st.session_state["company_id"] = ""
if "api_url" not in st.session_state:
    st.session_state["api_url"] = ""
if "producer" not in st.session_state:
    st.session_state["producer"] = None

# Open the pickle file in read binary mode
with open("website/models/best_model.pkl", "rb") as file:
    model = pickle.load(file)

def get_data(row):
    return {
        "unique_id": row['unique_id'],
        'transaction_time': get_time(),
        "transaction_date": get_date(),
        "step": row['step'],
        "amount": row['amount'],
        "oldbalanceOrig": row['oldbalanceOrig'],
        "newbalanceOrig": row['newbalanceOrig'],
        "oldbalanceDest": row['oldbalanceDest'],
        "newbalanceDest": row['newbalanceDest'],
        "type_Encoded": row['type_Encoded'],
        "type": row['type'],
    }

def send_to_kafka(producer, data):

    for index, row in data.iterrows():
        # df_row = pd.DataFrame([row])
        # df_row = df_row.drop(columns=['type', 'isFraud'])
        # pred = int(model.predict(df_row)[0])
        row['unique_id'] = uuid.uuid4()
        # row['Fraud'] = "yes" if pred == 1 else "no"
        data = get_data(row)

        # try:
        #     FRAUD_TOPIC = FRAUD_TOPIC+str(st.session_state.user_data["company_id"])
        # except:
        #     pass

        producer.produce(
            topic="fraud_test_topic_1",
            key=str(data['unique_id']),
            value=json.dumps(data, default=json_serializer).encode('utf-8'),
            on_delivery=delivery_report
        )
        
        producer.flush()
        time.sleep(10)
        yield row


# Define the main function for the Streamlit app
def main():
    st.title("Data Connect")
    
    if not st.session_state.load_state:

        api_url = st.text_input("API Endpoint URL", key="api_url", value=st.session_state["api_url"] if st.session_state["api_url"] is not None else "")
        csv_file = st.file_uploader("Upload CSV", type="csv", key="csv_file")
        # user_name = st.text_input("User Name", key="user_name", value=st.session_state["user_name"] if st.session_state["user_name"] is not None else "")
        # company_id = st.text_input("Company ID", key="company_id", value=st.session_state["company_id"] if st.session_state["company_id"] is not None else "")
        
        if st.button("Load"):
            st.session_state.load_state = True

    else:
        user_name = st.session_state.user_data["name"]
        company_id = st.session_state.user_data["company_id"]
        api_url = st.session_state["api_url"]

        st.write(f"User Name: {user_name}")
        st.write(f"Company ID: {company_id}")
        st.write(f"API Endpoint URL: {api_url}") 

        if st.session_state.csv_file is not None:
            data = pd.read_csv(st.session_state.csv_file)
        else:
            data = pd.read_json(st.session_state.api_url)
        
        # Placeholder for the table
        table_placeholder = st.empty()
        terminate_button = st.button("Terminate Sending Data")
        
        # DataFrame to keep track of the sent data
        sent_data = pd.DataFrame(columns=data.columns)

        for row in send_to_kafka(producer, data):
            if terminate_button:
                st.session_state.load_state = False
                break
            sent_data = pd.concat([sent_data, pd.DataFrame([row])], ignore_index=True)
            table_placeholder.table(sent_data)

if __name__ == "__main__":
    try:
        cluster_metadata = producer.list_topics(timeout=10)
        if not cluster_metadata.brokers:
            raise KafkaException(KafkaException)
                
        st.sidebar.success("Server is online.")
        
        # Check if the user is logged in
        try:
            if st.session_state.user is None:
                st.error("Please login to access this page.")
            else:
                main()
        except:
            st.error("Please login to access this page.")

    except KafkaException as e:
        st.error("Server offline. Please contact the administrator.")

    # except Exception as e:
    #     start_time = timedelta(hours=0, minutes=0, seconds=0)
    #     pass

