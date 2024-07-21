import streamlit as st
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time
import pickle

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers=['3.111.241.68:9092'], #change ip here
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

if "load_state" not in st.session_state:
    st.session_state.load_state = False

if "user_name" not in st.session_state:
    st.session_state["user_name"] = None
if "company_id" not in st.session_state:
    st.session_state["company_id"] = None
if "api_url" not in st.session_state:
    st.session_state["api_url"] = None

# Open the pickle file in read binary mode
with open("best_model.pkl", "rb") as file:
    model = pickle.load(file)

def send_to_kafka(data):
    i=0
    for index, row in data.iterrows():
        df_row = pd.DataFrame([row])
        df_row = df_row.drop(columns=['type', 'isFraud'])
        pred = int(model.predict(df_row)[0])
        row['Fraud'] = pred
        producer.send('from_firawd', value=str(row.to_json().encode('utf-8')))
        i+=1
        time.sleep(0.3)
        # producer.send('from_firawd', value={'surname':'parasdasdmar'})
        yield row

# Define logic function (to be implemented as per use case)
def logic():
    pass


# Define the main function for the Streamlit app
def main():
    st.title("API Connect")
    st.write("Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
              Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.")

    if not st.session_state.load_state:

        api_url = st.text_input("API Endpoint URL", key="api_url", value=st.session_state["api_url"] if st.session_state["api_url"] is not None else "")
        csv_file = st.file_uploader("Upload CSV", type="csv", key="csv_file")
        user_name = st.text_input("User Name", key="user_name", value=st.session_state["user_name"] if st.session_state["user_name"] is not None else "")
        company_id = st.text_input("Company ID", key="company_id", value=st.session_state["company_id"] if st.session_state["company_id"] is not None else "")
        
        if st.button("Load"):
            st.session_state.load_state = True
            logic()
    else:

        user_name = st.session_state["user_name"]
        company_id = st.session_state["company_id"]
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
        terminate_button = st.button("Terminate Production")
        
        # DataFrame to keep track of the sent data
        sent_data = pd.DataFrame(columns=data.columns)

        for row in send_to_kafka(data):
            if terminate_button:
                st.session_state.load_state = False
                break
            sent_data = pd.concat([sent_data, pd.DataFrame([row])], ignore_index=True)
            table_placeholder.table(sent_data)

if __name__ == "__main__":
    main()