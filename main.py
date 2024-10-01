from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import psycopg2
from pydantic import BaseModel

app = FastAPI()

# MongoDB connection
mongodb_client = MongoClient("mongodb://localhost:27017/")
db = mongodb_client["CustomerDataDB"]
collection = db["CustomerDocuments"]

# PostgreSQL connection
postgres_conn = psycopg2.connect(
    dbname="CustomerDB",
    user="postgres",
    password="bhavana27",
    host="127.0.0.1",
    port="5433"
)
cursor = postgres_conn.cursor()

# Pydantic model for MongoDB data
class CustomerData(BaseModel):
    Name: str

# Example endpoints
@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/mongodb/data")
def get_mongodb_data():
    data = list(collection.find())
    return data

@app.post("/mongodb/data")
def create_mongodb_data(data: CustomerData):
    try:
        collection.insert_one(data.dict())  # Convert Pydantic model to dict
        return {"message": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/postgres/data")
def get_postgres_data():
    cursor.execute("SELECT * FROM CustomerDB")
    data = cursor.fetchall()
    return data

@app.post("/postgres/data")
def create_postgres_data(data1: str, data2: str):
    try:
        cursor.execute("INSERT INTO CustomerDB (column1, column2) VALUES (%s, %s)", (data1, data2))
        postgres_conn.commit()  # Commit the changes
        return {"message": "Data inserted successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))  # Return error message

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
