import streamlit as st

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Welcome to Streamlit! 👋")

st.write("This project is a fraud detection project that uses real-time cloud tech to deliver near instant inferences. \
          The project uses a Kafka producer to send data to a Kafka topic, which is then consumed by a Kafka consumer. \
          To see this project in action, please navigate to the 'Connect' page.\nThanks! 😊")