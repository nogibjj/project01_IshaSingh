from fastapi import FastAPI
import uvicorn
from databricks import sql
import os

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello Databricks"}


@app.get("/add/{num1}/{num2}")
async def add(num1: int, num2: int):
    """Add two numbers together"""

    total = num1 + num2
    return {"total": total}


@app.get("/query")
async def query():
    """Execute a SQL query"""
    lst=[]
    with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM default.titanic_csv LIMIT 2")
            result = cursor.fetchall()

            for row in result:
                lst.append(row)

    result = lst
    return {"result": result}


if __name__ == "__main__":
    uvicorn.run(app, port=8080, host="0.0.0.0")

