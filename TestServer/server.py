from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras

from pymongo import MongoClient
from bson.json_util import dumps
import json
import os
from dotenv import load_dotenv

load_dotenv()


app = Flask(__name__)

# Database connection settings
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "new_password",
    "host": "localhost",
    "port": 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/execute_sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    query = data.get("query")

    # Safety: only allow SELECT queries (for now)
    if not query.lower().strip().startswith("select"):
        return jsonify({"error": "Only SELECT queries are allowed."}), 403

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
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


