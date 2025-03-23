import json
import logging
import traceback
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from io import BytesIO
from flask import send_file
from etl_lost_competitor import enrich_lost_customer_providers
import mysql.connector
import pandas as pd
import requests
from flask import Flask, render_template, request, jsonify, send_file
from flask_caching import Cache
from DynamicASNLink import update_missing_snapshots
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
from etl_caida import load_caida_data, dynamic_update, fill_lost_customers_simple
from etl_peeringdb import load_peeringdb_data

app = Flask(__name__)
app.config['CACHE_TYPE'] = 'simple'
cache = Cache(app)

logging.basicConfig(
    filename='asrank_dashboard.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

def maybe_update_peeringdb(asn):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True, buffered=True)
    sql_check = """
    SELECT last_update
    FROM peeringdb_networks
    WHERE asn=%s
    LIMIT 1
    """
    cursor.execute(sql_check, (asn,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row and row["last_update"]:
        last_update = row["last_update"]
        if (datetime.now() - last_update).total_seconds() < 86400:
            print(f"[INFO] PeeringDB data for ASN {asn} is recent, skipping update.")
            return
    try:
        load_peeringdb_data(asn)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("[WARN] Too many requests to PeeringDB. Skipping update for now.")
        else:
            raise

@app.route('/')
def index():
    return render_template('index.html')

@cache.cached(timeout=300, key_prefix=lambda: f"as_data_{request.view_args['asn']}")
@app.route('/api/as/<asn>', methods=['GET'])
def get_as_data(asn):
    snapshot_date = request.args.get('snapshot_date')
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        if snapshot_date:
            sql = "SELECT * FROM as_data WHERE asn=%s AND snapshot_date=%s"
            cursor.execute(sql, (asn, snapshot_date))
        else:
            sql = "SELECT * FROM as_data WHERE asn=%s ORDER BY snapshot_date DESC LIMIT 1"
            cursor.execute(sql, (asn,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if not result:
            logging.debug("No AS data found for ASN %s. Triggering dynamic update.", asn)
            try:
                dynamic_update(asn)
            except Exception as e:
                logging.error("Dynamic update failed for ASN %s: %s", asn, str(e))
                return jsonify({"error": f"Dynamic update failed: {str(e)}"}), 500
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True, buffered=True)
            if snapshot_date:
                sql = "SELECT * FROM as_data WHERE asn=%s AND snapshot_date=%s"
                cursor.execute(sql, (asn, snapshot_date))
            else:
                sql = "SELECT * FROM as_data WHERE asn=%s ORDER BY snapshot_date DESC LIMIT 1"
                cursor.execute(sql, (asn,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            if not result:
                logging.error("Data update triggered but still no data for ASN %s", asn)
                return jsonify({"error": "Data update triggered. Please refresh."}), 404
        return jsonify(result)
    except Exception as e:
        logging.error("Error in get_as_data endpoint: %s", traceback.format_exc())
        return jsonify({"error": "Error retrieving AS data", "details": str(e)}), 500

@app.route('/api/historical', methods=['GET'])
def historical_data():
    asn = request.args.get('asn')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not asn or not start_date or not end_date:
        return jsonify({"error": "asn, start_date, and end_date are required"}), 400
    if start_date and len(start_date) == 7:
        start_date = start_date + "-01"
    if end_date and len(end_date) == 7:
        end_date = end_date + "-01"
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        sql = """
        SELECT snapshot_date, as_rank, cone_asn_count, customer_degree, transit_degree, peer_degree, provider_degree
        FROM as_data
        WHERE asn=%s
          AND snapshot_date >= %s
          AND snapshot_date <= %s
        ORDER BY snapshot_date ASC
        """
        cursor.execute(sql, (asn, start_date, end_date))
        rows = cursor.fetchall()
        labels = [r["snapshot_date"].strftime("%Y-%m-%d") for r in rows]
        coneSizes = [r["cone_asn_count"] for r in rows]
        custQty = [r["customer_degree"] for r in rows]
        transDeg = [r["transit_degree"] for r in rows]
        peerQty = [r["peer_degree"] for r in rows]
        providerQty = [r["provider_degree"] for r in rows]  # Using the value directly
        ranks = [r["as_rank"] for r in rows]
        cursor.close()
        conn.close()
        return jsonify({
            "labels": labels,
            "coneSizes": coneSizes,
            "customerQuantity": custQty,
            "transitDegree": transDeg,
            "peerQuantity": peerQty,
            "providerQuantity": providerQty,
            "ranks": ranks
        })
    except Exception as e:
        logging.error("Error in historical_data: %s", traceback.format_exc())
        return jsonify({"error": "Error retrieving historical data", "details": str(e)}), 500


@app.route('/api/peeringdb/<asn>', methods=['GET'])
def get_peeringdb(asn):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        sql_net = "SELECT * FROM peeringdb_networks WHERE asn = %s"
        cursor.execute(sql_net, (asn,))
        net_record = cursor.fetchone()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Database error in get_peeringdb: %s", traceback.format_exc())
        return jsonify({"warning": f"Database error: {str(e)}"}), 500
    if not net_record:
        try:
            load_peeringdb_data(asn)
        except Exception as e:
            logging.error("Error updating PeeringDB for ASN %s: %s", asn, str(e))
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(sql_net, (asn,))
            net_record = cursor.fetchone()
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error("Database error re-checking PeeringDB: %s", traceback.format_exc())
            return jsonify({"warning": f"Database error: {str(e)}"}), 500
        if not net_record:
            return jsonify({"warning": "No PeeringDB data available for this ASN."})
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        sql_ix = """
        SELECT ix_id, name, ipaddr4, port_size, discovered_date
        FROM peeringdb_ix
        WHERE asn = %s
        ORDER BY ix_id ASC
        """
        cursor.execute(sql_ix, (asn,))
        ix_rows = cursor.fetchall()
        sql_fac = """
        SELECT fac_id, name, city, country, discovered_date
        FROM peeringdb_fac
        WHERE asn = %s
        ORDER BY fac_id ASC
        """
        cursor.execute(sql_fac, (asn,))
        fac_rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error retrieving PeeringDB child data: %s", traceback.format_exc())
        return jsonify({"warning": f"Database error: {str(e)}"}), 500
    net_record["netixlan_set"] = ix_rows
    net_record["netfac_set"] = fac_rows
    return jsonify(net_record)

@app.route('/api/available_snapshots', methods=['GET'])
def available_snapshots():
    asn = request.args.get('asn')
    if not asn:
        return jsonify({"error": "asn is required"}), 400
    try:
        conn = get_db_connection()
        cursor = conn.cursor(buffered=True)
        sql = "SELECT DISTINCT snapshot_date FROM as_data WHERE asn = %s ORDER BY snapshot_date ASC"
        cursor.execute(sql, (asn,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        snapshots = [row[0].strftime("%Y-%m-%d") for row in rows if row[0]]
        return jsonify({"snapshots": snapshots})
    except Exception as e:
        logging.error("Error in available_snapshots: %s", traceback.format_exc())
        return jsonify({"error": "Error retrieving available snapshots", "details": str(e)}), 500

@app.route('/api/update_data', methods=['POST'])
def update_data():
    data = request.json
    asn = data.get('asn')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    if not asn or not start_date or not end_date:
        return jsonify({"error": "Missing parameters: asn, start_date, and end_date are required."}), 400
    try:
        load_caida_data(asn, start_date, end_date)
    except Exception as e:
        logging.error("CAIDA ETL failed for ASN %s: %s", asn, traceback.format_exc())
        return jsonify({"error": f"CAIDA ETL failed: {str(e)}"}), 500
    try:
        fill_lost_customers_simple(asn)
    except Exception as e:
        logging.error("Error filling lost customers for ASN %s: %s", asn, traceback.format_exc())
    try:
        maybe_update_peeringdb(asn)
    except Exception as e:
        logging.error("PeeringDB ETL failed for ASN %s: %s", asn, traceback.format_exc())
        return jsonify({"error": f"PeeringDB ETL failed: {str(e)}"}), 500
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = "INSERT INTO etl_metadata (process_name, status, details) VALUES (%s, %s, %s)"
        params = (f"Manual_ETL for ASN {asn}", "successful", f"Manual update for ASN {asn} from {start_date} to {end_date}")
        cursor.execute(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error inserting ETL metadata: %s", traceback.format_exc())
    return jsonify({"message": f"Manual update triggered for ASN {asn} from {start_date} to {end_date}."})

# --- Competitor Analysis Functions ---

def find_competitors(requestor_asn, lost_asn, lostDate):
    # Ensure lostDate is a date object.
    if hasattr(lostDate, "date"):
        lostDate = lostDate.date()

    # Entire window: from (lostDate - 3 months) to (lostDate + 3 months)
    full_start = (lostDate - relativedelta(months=3)).replace(day=1)
    full_end   = (lostDate + relativedelta(months=3)).replace(day=1)

    # Define baseline and current boundaries:
    baseline_start = full_start          # Baseline: from (lostDate - 3 months)...
    baseline_end   = lostDate             # ...up to lostDate (exclusive)
    current_start  = lostDate             # Current: from lostDate...
    current_end    = full_end             # ...up to (lostDate + 3 months) (exclusive)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True, buffered=True)

    # Query the as_relationships table in the entire ±3-month window.
    sql = """
      SELECT provider_asn,
             snapshot_date,
             (SELECT asn_name
                FROM as_data
               WHERE asn = r.provider_asn
                 AND snapshot_date = r.snapshot_date
               LIMIT 1) AS provider_name
      FROM as_relationships r
      WHERE customer_asn = %s
        AND relationship_type = 'provider'
        AND snapshot_date >= %s
        AND snapshot_date < %s
    """
    cursor.execute(sql, (lost_asn, full_start.strftime("%Y-%m-%d"), full_end.strftime("%Y-%m-%d")))
    entire_range_rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Build dictionaries and sets.
    earliest_date_map = {}  # Maps provider_asn -> earliest snapshot_date in window.
    provider_name_map = {}  # Maps provider_asn -> provider name.
    baseline_providers = set()
    current_providers  = set()

    for row in entire_range_rows:
        pasn = row["provider_asn"]
        pdate = row["snapshot_date"]
        # Convert snapshot_date to a date if it is a datetime.
        if hasattr(pdate, "date"):
            pdate = pdate.date()
        pname = row["provider_name"] if row["provider_name"] else "N/A"

        # Track the earliest date for this provider.
        if pasn not in earliest_date_map or pdate < earliest_date_map[pasn]:
            earliest_date_map[pasn] = pdate

        # Track provider name.
        if pasn not in provider_name_map:
            provider_name_map[pasn] = pname

        # Categorize providers into baseline and current groups.
        if baseline_start <= pdate < baseline_end:
            baseline_providers.add(pasn)
        if current_start <= pdate < current_end:
            current_providers.add(pasn)

    # New competitors: providers in current window that are NOT in baseline.
    new_asns = {asn for asn in current_providers if earliest_date_map[asn] >= lostDate}

    comp_asn = list(new_asns)
    comp_org = [provider_name_map.get(asn_val, "N/A") for asn_val in new_asns]

    if new_asns:
        earliest_new_date = min(earliest_date_map[asn_val] for asn_val in new_asns)
        comp_date = earliest_new_date.strftime("%b-%Y")  # e.g., "Jan-2025"
    else:
        comp_date = None

    return comp_asn, comp_org, comp_date

@app.route('/api/competitor_analysis', methods=['GET'])
def competitor_analysis():
    try:
        asn = request.args.get('requestor_asn')
        enrich_lost_customer_providers(asn)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        if start_date and len(start_date) == 7:
            start_date = start_date + "-01"
        if end_date and len(end_date) == 7:
            end_date = end_date + "-01"
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True, buffered=True)
        conds = ["requestor_asn=%s"]
        vals = [asn]
        if start_date:
            conds.append("lost_month >= %s")
            vals.append(start_date)
        if end_date:
            conds.append("lost_month <= %s")
            vals.append(end_date)
        where_clause = " AND ".join(conds)
        sql = f"""
          SELECT id, requestor_asn, lost_asn, lost_month, lost_org, lost_cust_cone
          FROM lost_customers
          WHERE {where_clause}
          ORDER BY lost_month
        """
        cursor.execute(sql, tuple(vals))
        lost_rows = cursor.fetchall()
        cursor.close()
        conn.close()
        if not lost_rows:
            return jsonify({"totalLostCount": 0, "monthlyStats": []})
        from collections import defaultdict
        grouped = defaultdict(list)
        for row in lost_rows:
            m_val = row["lost_month"]
            if isinstance(m_val, date):
                key = m_val
            else:
                key = datetime.strptime(str(m_val), "%Y-%m-%d").date()
            grouped[key].append(row)
        monthlyStats = []
        totalLostCount = len(lost_rows)
        for mKey in sorted(grouped.keys()):
            dt = datetime.combine(mKey, datetime.min.time())
            prevMonth = dt - relativedelta(months=1)
            prevCone = getCone(asn, prevMonth)
            currCone = getCone(asn, dt)
            prevConeVal = prevCone if prevCone is not None else "N/A"
            currConeVal = currCone if currCone is not None else "N/A"
            if isinstance(prevConeVal, int) and isinstance(currConeVal, int):
                coneChanges = currConeVal - prevConeVal
            else:
                coneChanges = "N/A"
            lostEvents = []
            for row in grouped[mKey]:
                relationship_cm = find_relationship_cm(asn, row["lost_asn"], dt)
                comp_asn, comp_org, comp_date = find_competitors(asn, row["lost_asn"], dt)
                lostEvents.append({
                    "lost_asn": row["lost_asn"],
                    "lost_org": row["lost_org"],
                    "lost_cust_cone": row["lost_cust_cone"],
                    "relationship_cm": relationship_cm,
                    "competitor_asn": comp_asn,
                    "competitor_org": comp_org,
                    "competitor_month": comp_date if comp_date else dt.strftime("%b-%Y")
                })
            monthlyStats.append({
                "month": dt.strftime("%Y-%m-%d"),
                "previousMonthCone": prevConeVal,
                "currentCone": currConeVal,
                "coneChanges": coneChanges,
                "lostEvents": lostEvents
            })
        return jsonify({
            "totalLostCount": totalLostCount,
            "monthlyStats": monthlyStats
        })
    except Exception as e:
        logging.error("Error in competitor_analysis endpoint: %s", traceback.format_exc())
        return jsonify({"error": "Competitor analysis failed", "details": str(e)}), 500

def getCone(asn, dateObj):
    dateStr = dateObj.replace(day=1).strftime("%Y-%m-%d")
    val = queryCone(asn, dateStr)
    return val

def queryCone(asn, dateStr):
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    cursor.execute("SELECT cone_asn_count FROM as_data WHERE asn=%s AND snapshot_date=%s", (asn, dateStr))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

def find_relationship_cm(requestor_asn, lost_asn, dateObj):
    dateStr = dateObj.replace(day=1).strftime("%Y-%m-%d")
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    cursor.execute("""
      SELECT relationship_type
      FROM as_relationships
      WHERE provider_asn=%s AND customer_asn=%s AND snapshot_date=%s
    """, (requestor_asn, lost_asn, dateStr))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else "none"

@app.route('/api/run_lost_competitor_etl', methods=['POST'])
def run_lost_competitor_etl():
    data = request.json
    asn = data.get('asn')
    if not asn:
        return jsonify({"error": "ASN is required"}), 400
    try:
        from etl_lost_competitor import compute_lost_competitor_data
        compute_lost_competitor_data(asn)
        return jsonify({"message": f"Lost competitor ETL triggered for ASN {asn}."})
    except Exception as e:
        logging.error("Error in run_lost_competitor_etl: %s", traceback.format_exc())
        return jsonify({"error": "Failed to run lost competitor ETL", "details": str(e)}), 500

# NEW: Excel export endpoint for raw data download and consolidated views
@app.route('/api/download_raw_data', methods=['GET'])
def download_raw_data():
    asn = request.args.get('asn')
    if not asn:
        return jsonify({"error": "ASN parameter is required"}), 400

    conn = get_db_connection()

    # Query raw data for each sheet
    hist_df = pd.read_sql("SELECT * FROM as_data WHERE asn = %s ORDER BY snapshot_date ASC", conn, params=(asn,))
    lost_df = pd.read_sql("SELECT * FROM lost_customers WHERE requestor_asn = %s ORDER BY lost_month ASC", conn, params=(asn,))
    rel_df = pd.read_sql("SELECT * FROM as_relationships WHERE provider_asn = %s OR customer_asn = %s ORDER BY snapshot_date ASC", conn, params=(asn, asn))
    net_df = pd.read_sql("SELECT * FROM peeringdb_networks WHERE asn = %s", conn, params=(asn,))
    ix_df = pd.read_sql("SELECT * FROM peeringdb_ix WHERE asn = %s", conn, params=(asn,))
    fac_df = pd.read_sql("SELECT * FROM peeringdb_fac WHERE asn = %s", conn, params=(asn,))
    conn.close()

    # Create a multi-sheet Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        hist_df.to_excel(writer, index=False, sheet_name="Historical AS Data")
        lost_df.to_excel(writer, index=False, sheet_name="Lost Customers")
        rel_df.to_excel(writer, index=False, sheet_name="AS Relationships")
        net_df.to_excel(writer, index=False, sheet_name="PeeringDB Networks")
        ix_df.to_excel(writer, index=False, sheet_name="PeeringDB Exchanges")
        fac_df.to_excel(writer, index=False, sheet_name="PeeringDB Facilities")
    output.seek(0)
    filename = f"raw_data_{asn}.xlsx"
    return send_file(output, attachment_filename=filename,
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
if __name__ == '__main__':
    app.run(debug=True)

from DynamicASNLink import update_missing_snapshots

@app.route('/api/download_caida_data', methods=['GET'])
def download_caida_data():
    asn = request.args.get('asn')
    start_date = request.args.get('start_date')  # Expected format: YYYY-MM-DD
    end_date = request.args.get('end_date')      # Expected format: YYYY-MM-DD
    if not asn or not start_date or not end_date:
        return jsonify({"error": "asn, start_date, and end_date are required"}), 400

    # Optionally, update missing snapshots before exporting:
    try:
        from DynamicASNLink import update_missing_snapshots
        update_missing_snapshots(asn, start_date, end_date)
    except Exception as e:
        logging.error("Error updating missing snapshots for ASN %s: %s", asn, str(e))
        # Depending on your needs, you might proceed even if this fails.

    conn = get_db_connection()

    # Query CAIDA snapshots for the given ASN and date range
    query_snapshots = """
        SELECT *
        FROM caida_snapshots
        WHERE asn = %s AND snapshot_date BETWEEN %s AND %s
        ORDER BY snapshot_date ASC
    """
    try:
        import pandas as pd
        df_snapshots = pd.read_sql(query_snapshots, conn, params=(asn, start_date, end_date))
    except Exception as e:
        conn.close()
        return jsonify({"error": f"Error querying caida_snapshots: {str(e)}"}), 500

    # Query CAIDA snapshot links for the given ASN and date range
    query_links = """
        SELECT *
        FROM caida_snapshot_links
        WHERE parent_asn = %s AND snapshot_date BETWEEN %s AND %s
        ORDER BY snapshot_date ASC
    """
    try:
        df_links = pd.read_sql(query_links, conn, params=(asn, start_date, end_date))
    except Exception as e:
        conn.close()
        return jsonify({"error": f"Error querying caida_snapshot_links: {str(e)}"}), 500
    conn.close()

    # If both DataFrames are empty, report error.
    if df_snapshots.empty and df_links.empty:
        return jsonify({"error": "No CAIDA snapshot data available for the selected date range."}), 404

    # Create a multi-sheet Excel file with both DataFrames.
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if not df_snapshots.empty:
            df_snapshots.to_excel(writer, index=False, sheet_name="CAIDA Snapshots")
        else:
            pd.DataFrame().to_excel(writer, index=False, sheet_name="CAIDA Snapshots")
        if not df_links.empty:
            df_links.to_excel(writer, index=False, sheet_name="CAIDA Snapshot Links")
        else:
            pd.DataFrame().to_excel(writer, index=False, sheet_name="CAIDA Snapshot Links")
    output.seek(0)
    filename = f"caida_data_{asn}_{start_date}_to_{end_date}.xlsx"
    return send_file(output, attachment_filename=filename,
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


