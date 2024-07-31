import streamlit as st
import json

st.set_page_config(
    page_title="FraudCatch Home",
    page_icon="👋",
)

try:
    if st.session_state.user_data is not None:
        st.write(f"# Welcome back to Fraudcatch `alpha`, `{st.session_state.user_data['name']}`!")

except AttributeError as err:
    st.write("# Welcome to FraudCatch `alpha`! 👋")

st.markdown("FraudCatch `alpha` is a fraud detection project that uses real-time cloud tech to deliver near instant inferences. \
          The project uses a Kafka producer to send data to a Kafka topic, which is then consumed by a Kafka consumer. \
          We have used distributed processing using Apache Spark to process the data and make predictions using a trained model. \
          To see this project in action, please navigate to the 'Connect' page.\nThanks! 😊")

