from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
from bson import ObjectId

app = Flask(__name__)

# ── CHANGE THIS to your MongoDB Atlas connection string ──────────────────────
MONGO_URI = "mongodb+srv://imartinezdelariva_db_user:fBeeXmzTR7sEPGsX@cluster0.npmwf29.mongodb.net/?appName=Cluster0"
# ─────────────────────────────────────────────────────────────────────────────

client = MongoClient(MONGO_URI)
db = client["ev_db"]
collection = db["vehicles"]


# 1. Fast but Unsafe Write
@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    data = request.get_json()
    fast_collection = collection.with_options(
        write_concern=WriteConcern(w=1)
    )
    result = fast_collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)}), 201


# 2. Highly Durable Write
@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    data = request.get_json()
    safe_collection = collection.with_options(
        write_concern=WriteConcern(w="majority")
    )
    result = safe_collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)}), 201


# 3. Strongly Consistent Read
@app.route("/count-tesla-primary", methods=["GET"])
def count_tesla_primary():
    primary_collection = collection.with_options(
        read_preference=ReadPreference.PRIMARY
    )
    count = primary_collection.count_documents({"Make": "TESLA"})
    return jsonify({"count": count}), 200


# 4. Eventually Consistent Analytical Read
@app.route("/count-bmw-secondary", methods=["GET"])
def count_bmw_secondary():
    secondary_collection = collection.with_options(
        read_preference=ReadPreference.SECONDARY_PREFERRED
    )
    count = secondary_collection.count_documents({"Make": "BMW"})
    return jsonify({"count": count}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
