import pymongo
import datetime
import streamlit as st

def connect_db():
    conn_str = "mongodb://localhost:27017/"
    try:
        mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        print("Pymongo : connection OK")
        db = mongo_client.gdelt
        st.session_state['coll'] = db.evt
        print(f"Count documents : {st.session_state['coll'].estimated_document_count()}")
        return [True, st.session_state['coll']]
    except Exception as e:
        print(e)
        print("Pymongo : unable to connect to the server.")
        return [False, None]