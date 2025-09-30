from flask import Flask, render_template, request, jsonify
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from decimal import Decimal
import json
import os
import hashlib
import atexit

# Database config (USE ENV VARIABLES IN PRODUCTION)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://smriti:14IoOzwofyHsi1RhXlnC2g@transaction-flow-16380.j77.aws-ap-south-1.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
)
TABLE_NAME = "erc20_transfers"
DISPERSE_TABLE = "disperse"
DISPERSE_CONTRACT = "0xd152f549545093347a162dce210e7293f1452150".lower()

# Connection pool for better performance
connection_pool = None

def init_pool():
    """Initialize connection pool."""
    global connection_pool
    if connection_pool is None:
        connection_pool = SimpleConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=DATABASE_URL
        )

def close_pool():
    """Close connection pool."""
    global connection_pool
    if connection_pool is not None:
        connection_pool.closeall()
        connection_pool = None

# Register cleanup on exit
atexit.register(close_pool)

app = Flask(__name__)

# Custom JSON encoder to handle Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

app.json_encoder = DecimalEncoder

def get_db_connection():
    """Get a connection from the pool."""
    if connection_pool is None:
        init_pool()
    return connection_pool.getconn()

def return_db_connection(conn):
    """Return a connection to the pool."""
    if connection_pool is not None:
        connection_pool.putconn(conn)

def get_disperse_tx_hashes_optimized(conn, from_address):
    """
    Get all transaction hashes where from_address sent to disperse contract.
    Optimized query with minimal data transfer.
    """
    query = f"""
        SELECT tx_hash
        FROM {TABLE_NAME}
        WHERE from_address = %s
        AND to_address = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (from_address, DISPERSE_CONTRACT))
        results = cur.fetchall()
    
    return [row[0] for row in results]

def get_disperse_beneficiaries_optimized(conn, tx_hashes):
    """
    Get beneficiaries and their amounts from disperse table for given tx_hashes.
    Returns dict with to_address as key and total amount as value, plus tx count.
    Optimized with batch processing.
    """
    if not tx_hashes:
        return {}, 0
    
    # Use unnest for better performance with large arrays
    query = f"""
        SELECT 
            to_address,
            SUM(value) as total_value
        FROM {DISPERSE_TABLE}
        WHERE tx_hash = ANY(%s)
        GROUP BY to_address
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (tx_hashes,))
        results = cur.fetchall()
    
    beneficiaries = {}
    for row in results:
        to_addr = row[0].lower() if row[0] else row[0]
        value = float(row[1]) if row[1] else 0.0
        beneficiaries[to_addr] = value
    
    return beneficiaries, len(tx_hashes)

# Simple in-memory cache (use Redis for production)
_cache = {}
CACHE_SIZE = 1000

def get_cache_key(address):
    """Generate cache key for address."""
    return hashlib.md5(address.encode()).hexdigest()

def get_from_cache(address):
    """Get result from cache if available."""
    key = get_cache_key(address)
    return _cache.get(key)

def set_to_cache(address, data):
    """Set result to cache with size limit."""
    if len(_cache) >= CACHE_SIZE:
        # Remove oldest entry (simple FIFO)
        _cache.pop(next(iter(_cache)))
    key = get_cache_key(address)
    _cache[key] = data

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/trace", methods=["POST"])
def trace_flow():
    data = request.get_json()
    address = data.get("address", "").strip()
    use_cache = data.get("use_cache", True)  # Allow disabling cache
    
    if not address:
        return jsonify({"error": "Address is required"}), 400
    
    if not address.startswith("0x"):
        return jsonify({"error": "Invalid address format. Address must start with '0x'"}), 400
    
    address = address.lower()
    
    # Check cache first
    if use_cache:
        cached_result = get_from_cache(address)
        if cached_result:
            cached_result["cached"] = True
            return jsonify(cached_result)
    
    conn = None
    try:
        conn = get_db_connection()
        
        # Query 1: Get transaction hashes (MUST be separate to get accurate count)
        disperse_tx_hashes = get_disperse_tx_hashes_optimized(conn, address)
        
        # Query 2: Get beneficiaries from those transactions (MUST be separate)
        disperse_beneficiaries, tx_count = get_disperse_beneficiaries_optimized(
            conn, disperse_tx_hashes
        ) if disperse_tx_hashes else ({}, 0)
        
        # Calculate total
        total_disperse = sum(disperse_beneficiaries.values())
        
        result = {
            "success": True,
            "address": address,
            "disperse_beneficiaries": disperse_beneficiaries,
            "stats": {
                "disperse_beneficiaries": len(disperse_beneficiaries),
                "total_disperse_amount": total_disperse,
                "disperse_transactions": tx_count
            },
            "cached": False
        }
        
        # Cache the result
        if use_cache:
            set_to_cache(address, result)
        
        return jsonify(result)
        
    except psycopg2.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if conn:
            return_db_connection(conn)

@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    """Endpoint to clear cache."""
    _cache.clear()
    return jsonify({"success": True, "message": "Cache cleared"})

@app.route("/cache/stats", methods=["GET"])
def cache_stats():
    """Get cache statistics."""
    return jsonify({
        "cache_size": len(_cache),
        "cache_limit": CACHE_SIZE
    })

@app.before_request
def before_request():
    """Initialize pool before first request."""
    if connection_pool is None:
        init_pool()

if __name__ == "__main__":
    # Initialize pool on startup
    init_pool()
    try:
        app.run(debug=True, host="0.0.0.0", port=5000)
    finally:
        close_pool()