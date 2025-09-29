from flask import Flask, render_template, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
import json

# Database config (USE ENV VARIABLES IN PRODUCTION)
DATABASE_URL = "postgresql://smriti:14IoOzwofyHsi1RhXlnC2g@transaction-flow-16380.j77.aws-ap-south-1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
TABLE_NAME = "erc20_transfers"
DISPERSE_TABLE = "disperse"
DISPERSE_CONTRACT = "0xd152f549545093347a162dce210e7293f1452150".lower()

app = Flask(__name__)

# Custom JSON encoder to handle Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

app.json_encoder = DecimalEncoder

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(DATABASE_URL)

def get_disperse_tx_hashes(conn, from_address):
    """
    Get all transaction hashes where from_address sent to disperse contract.
    """
    query = f"""
        SELECT DISTINCT tx_hash
        FROM {TABLE_NAME}
        WHERE LOWER(from_address) = %s
        AND LOWER(to_address) = %s
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (from_address.lower(), DISPERSE_CONTRACT))
        results = cur.fetchall()
    
    return [row['tx_hash'] for row in results]

def get_disperse_beneficiaries(conn, tx_hashes):
    """
    Get beneficiaries and their amounts from disperse table for given tx_hashes.
    Returns a dict with to_address as key and total amount as value.
    """
    if not tx_hashes:
        return {}
    
    query = f"""
        SELECT to_address, SUM(value) as total_value
        FROM {DISPERSE_TABLE}
        WHERE tx_hash = ANY(%s)
        GROUP BY to_address
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (tx_hashes,))
        results = cur.fetchall()
    
    return {row['to_address'].lower(): float(row['total_value']) for row in results}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/trace", methods=["POST"])
def trace_flow():
    data = request.get_json()
    address = data.get("address", "").strip()
    
    if not address:
        return jsonify({"error": "Address is required"}), 400
    
    if not address.startswith("0x"):
        return jsonify({"error": "Invalid address format. Address must start with '0x'"}), 400
    
    address = address.lower()
    
    try:
        conn = get_db_connection()
        
        # Get disperse transactions
        disperse_tx_hashes = get_disperse_tx_hashes(conn, address)
        disperse_beneficiaries = get_disperse_beneficiaries(conn, disperse_tx_hashes) if disperse_tx_hashes else {}
        
        conn.close()
        
        # Calculate totals
        total_disperse = sum(disperse_beneficiaries.values())
        
        return jsonify({
            "success": True,
            "address": address,
            "disperse_beneficiaries": disperse_beneficiaries,
            "stats": {
                "disperse_beneficiaries": len(disperse_beneficiaries),
                "total_disperse_amount": total_disperse,
                "disperse_transactions": len(disperse_tx_hashes)
            }
        })
        
    except Exception as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)