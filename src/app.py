from fastapi import FastAPI
from bs4 import BeautifulSoup
import mysql.connector 
import requests
from config import *

app = FastAPI()

# Web scraping
url = "https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx"
try: 
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
except Exception as error:
    print(str(error))
    raise

# Extracting the data
table = soup.find("table", {"id": "bulkDealsTable"})
rows = table.find_all("tr")

data = []
for row in rows[1:]:
    cells = row.find_all("td")
    date = cells[0].text.strip()
    company = cells[1].text.strip()
    quantity = cells[2].text.strip()
    price = cells[3].text.strip()
    data.append((date, company, quantity, price))

# MySQL database connection
try:
    db = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        database=db
    )
    cursor = db.cursor()
except Exception as error:
    print("There is an error in this connection "+ str(error))

# Create the user table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255)
    )
""")


# Create a user
@app.post("/users")
def create_user(name: str, email: str):
    query = "INSERT INTO users (name, email) VALUES (%s, %s)"
    values = (name, email)
    cursor.execute(query, values)
    db.commit()
    return {"message": "User created successfully"}

# Read a user
@app.get("/users/{user_id}")
def read_user(user_id: int):
    query = "SELECT * FROM users WHERE id = %s"
    values = (user_id,)
    cursor.execute(query, values)
    result = cursor.fetchone()
    if result:
        user = {
            "id": result[0],
            "name": result[1],
            "email": result[2]
        }
        return user
    return {"message": "User not found"}

# Update a user
@app.put("/users/{user_id}")
def update_user(user_id: int, name: str, email: str):
    query = "UPDATE users SET name = %s, email = %s WHERE id = %s"
    values = (name, email, user_id)
    cursor.execute(query, values)
    db.commit()
    return {"message": "User updated successfully"}

# Delete a user
@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    query = "DELETE FROM users WHERE id = %s"
    values = (user_id,)
    cursor.execute(query, values)
    db.commit()
    return {"message": "User deleted successfully"}
