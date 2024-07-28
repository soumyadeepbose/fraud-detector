import streamlit as st
import pyrebase
import os
import uuid
#import firebase_admin
#from firebase_admin import credentials, auth

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


def signup():
    st.subheader("Create New Account")
    name = st.text_input("Name")
    company_id = st.text_input("Company ID")
    email = st.text_input("Email")
    password = st.text_input("Password", type='password')
    confirm_password = st.text_input("Confirm Password", type='password')
    
    if st.button("SignUp") and password == confirm_password:
        try:
            uu_id = uuid.uuid4().hex
            user = auth.create_user_with_email_and_password(email, password)
            st.success(f"Account created successfully!\n \
                       Your unique user ID is: {uu_id}")
            
            # Store additional user info in the database
            user_data = {
                "name": name,
                "company_id": company_id,
                "user_id": uu_id,
                "email": email
            }
            db.child("users").child(user['localId']).set(user_data)
            
            st.info("You can now login using your email and password.")
        except:
            st.error("Unable to create account. Please try again.")
    else:
        st.error("Passwords do not match. Please try again.")

def login():
    st.subheader("Login to Your Account")
    email = st.text_input("Email")
    password = st.text_input("Password", type='password')
    
    if st.button("Login"):
        try:
            user = auth.sign_in_with_email_and_password(email, password)
            st.success("Logged in successfully!")
            st.session_state.user = user
            st.session_state.signedout = False
            st.experimental_rerun()
            
        except:
            st.error("Login failed. Please check your credentials.")
    
def main():
    # Initialize session state variables
    if 'user' not in st.session_state:
        st.session_state.user = None
    if 'signedout' not in st.session_state:
        st.session_state.signedout = False
    
    menu = ["Login", "SignUp"]
    choice = st.sidebar.selectbox("Menu", menu)
    
    if choice == "Login":
        if st.session_state.user is None:
            login()
        else:
            st.info("You are already logged in.")
            if st.button("Go to Home"):
                st.experimental_rerun()
    elif choice == "SignUp":
        if st.session_state.user is None:
            signup()
        else:
            st.info("You are already logged in. Please log out to create a new account.")
    
    # Add a logout button in the sidebar
    if st.session_state.user is not None:
        if st.sidebar.button("Logout"):
            st.session_state.user = None
            st.session_state.signedout = True
            st.info("You have been logged out")
            st.experimental_rerun()

if __name__ == '__main__':
    main()