
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from pymongo import MongoClient
from bson.json_util import dumps
import json
import os
from dotenv import load_dotenv

load_dotenv()


app = Flask(__name__)

# Database connection settings
# DB_CONFIG = {
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "new_password",
#     "host": "localhost",
#     "port": 5432
# }

DB_URL = "postgresql+psycopg2://postgres:new_password@localhost:5432/postgres"
engine = create_engine(DB_URL)

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    query = data.get('query')

    if not query:
        return jsonify({"error": "Missing SQL query"}), 400

    try:
        with engine.begin() as conn:  # transaction auto-commit
            result = conn.execute(text(query))
            if result.returns_rows:
                rows = [dict(row) for row in result.mappings().all()]
                return jsonify({"result": rows})
            else:
                return jsonify({"message": "Query executed successfully"})
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

    

# MongoDB connection
client = MongoClient(os.getenv("MONGO_URI"))  # change if using a remote DB
db = client["test"]  # replace with your DB name
collection = db["test"]  # replace with your collection name

@app.route('/query_mongo', methods=['POST'])
def query_mongo():
    try:
        # Client sends a JSON body like: {"age": {"$gt": 25}, "name": "Alice"}
        data = request.get_json()

        filters = data.get('query')
        
        print(filters)

        results = collection.find(filters)
        json_results = json.loads(dumps(results))

        return jsonify(json_results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route("/mongo_query_aggregate", methods=["POST"])
def mongo_query():
    try:
        data = request.json
        
        if "aggregate" in data and "pipeline" in data:
            collection_name = data["aggregate"]
            pipeline = data["pipeline"]
            
            collection = db[collection_name]
            result = list(collection.aggregate(pipeline))
        elif "find" in data:
            collection_name = data["find"]
            query = data.get("filter", {})
            
            collection = db[collection_name]
            result = list(collection.find(query))
        else:
            return jsonify({"error": "Invalid format. Use 'aggregate' or 'find' with appropriate query."}), 400
        
        return dumps(result), 200
    

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)


