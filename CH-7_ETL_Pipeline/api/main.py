from fastapi import FastAPI,status
import uvicorn


data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35}
]


app = FastAPI()

@app.get("/",status_code=status.HTTP_200_OK)
def home():
    return {"message": "Welcome to the FastAPI application!"}

@app.get("/fetch_data",status_code=status.HTTP_200_OK)
def fetch_data():
    return {"data": data}


if __name__ == "__main__":
    
    uvicorn.run(app, host="0.0.0.0", port=8000)

