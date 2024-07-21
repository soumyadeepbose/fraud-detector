import streamlit as st
import pyrebase
import os

# Simulated database
users_db = {}

firebase_api = os.getenv('FIREBASE_API')

firebaseConfig = {
  'apiKey': firebase_api,
  'authDomain': "humanaize-e91b0.firebaseapp.com",
  'projectId': "humanaize-e91b0",
  'storageBucket': "humanaize-e91b0.appspot.com",
  'messagingSenderId': "616845459816",
  'appId': "1:616845459816:web:b051ca72ba0ba1e5c096dd",
  'databaseURL': "https://humanaize-e91b0-default-rtdb.europe-west1.firebasedatabase.app/"
}

firebase = pyrebase.initialize_app(firebaseConfig)
auth= firebase.auth()

db= firebase.database()
storage= firebase.storage()

def sign_up(email, password):
    try:
        user = auth.create_user_with_email_and_password(email, password)
        st.success("User created successfully!")
        return user
    except Exception as e:
        st.error(f"Error: {e}")

def login(email, password):
    try:
        user = auth.sign_in_with_email_and_password(email, password)
        st.success("Logged in successfully!")
        return user
    except Exception as e:
        st.error(f"Error: {e}")
        return None

# Streamlit app
st.title("Login Page")

# Choose between signup and login
choice = st.sidebar.selectbox("Select an option", ["Signup", "Login"])

if choice == "Signup":
    st.subheader("Signup")
    username = st.text_input("Mail ID")
    password = st.text_input("Password", type="password")
    if st.button("Signup"):
        message = signup(username, password)
        st.success(message)

elif choice == "Login":
    st.subheader("Login")
    username = st.text_input("Mail ID")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        message = login(username, password)
        st.success(message)
