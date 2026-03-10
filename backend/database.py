import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",         # Your MySQL username
        password="Subiksha@12345", # Your MySQL password
        database="scheduling_system"
    )