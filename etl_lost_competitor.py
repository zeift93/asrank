import requests
import mysql.connector
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

# CAIDA GraphQL API endpoint
CAIDA_API_URL = "https://api.asrank.caida.org/v2/graphql"

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

def normalize_date_to_month(date_str):
    """
    Convert a date string (e.g. 2024-03-15) to first-of-month format (2024-03-01).
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(day=1).strftime("%Y-%m-%d")

def fetch_caida_data(asn, date_start, date_end):
    """
    Uses an expanded GraphQL query to retrieve:
      - asn, asnName, rank, date
      - country { iso, name, capital, population, continent }
      - organization { orgName }
      - asnDegree { customer, peer, transit }
      - cone { numberAsns, numberPrefixes, numberAddresses }
      - asnLinks { edges { node { ... asn1 { ... } } } }
        including orgName, country, rank, cone, asnDegree for asn1
    """
    query_text = f"""
    {{
      asns(asns:["{asn}"], dateStart:"{date_start}", dateEnd:"{date_end}") {{
        edges {{
          node {{
            asn
            asnName
            rank
            date
            country {{
              iso
              name
              capital
              population
              continent
            }}
            organization {{
              orgName
            }}
            asnDegree {{
              customer
              peer
              transit
              provider
            }}
            cone {{
              numberAsns
              numberPrefixes
              numberAddresses
            }}
            asnLinks {{
              edges {{
                node {{
                  numberPaths
                  relationship
                  asn1 {{
                    country {{
                      iso
                      name
                      capital
                      population
                      continent
                    }}
                    organization {{
                      orgName
                    }}
                    asn
                    asnName
                    rank
                    cone {{
                      numberAsns
                      numberPrefixes
                      numberAddresses
                    }}
                    asnDegree {{
                      customer
                      peer
                      transit
                      provider
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """

    headers = {
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip"
    }
    resp = requests.post(
        CAIDA_API_URL,
        json={"query": query_text},
        headers=headers,
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()

def load_caida_data(asn, date_start, date_end):
    """
    Load CAIDA data for the given ASN between date_start and date_end,
    storing expanded fields (org_name, country, etc.) into as_data.
    Also inserts link nodes (asn1) into as_data, so we have org/country info
    for potential lost customers as well.
    """
    print(f"[INFO] Loading CAIDA data for ASN {asn} from {date_start} to {date_end}")
    data = fetch_caida_data(asn, date_start, date_end)
    nodes = data.get("data", {}).get("asns", {}).get("edges", [])
    if not nodes:
        print(f"[WARN] No data returned for ASN {asn} between {date_start} and {date_end}")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL for inserting as_data with new fields
    sql_as_data_insert = """
    INSERT INTO as_data (
        asn,
        snapshot_date,
        asn_name,
        org_name,
        country_iso,
        country_name,
        country_capital,
        country_population,
        country_continent,
        as_rank,
        cone_asn_count,
        cone_prefix_count,
        cone_address_count,
        customer_degree,
        peer_degree,
        transit_degree,
        provider_degree,
        last_update
    )
    VALUES (
        %(asn)s,
        %(snapshot_date)s,
        %(asn_name)s,
        %(org_name)s,
        %(country_iso)s,
        %(country_name)s,
        %(country_capital)s,
        %(country_population)s,
        %(country_continent)s,
        %(as_rank)s,
        %(cone_asn_count)s,
        %(cone_prefix_count)s,
        %(cone_address_count)s,
        %(cust_deg)s,
        %(peer_deg)s,
        %(trans_deg)s,
        %(provider_deg)s,
        NOW()
    )
    ON DUPLICATE KEY UPDATE
        asn_name = VALUES(asn_name),
        org_name = VALUES(org_name),
        country_iso = VALUES(country_iso),
        country_name = VALUES(country_name),
        country_capital = VALUES(country_capital),
        country_population = VALUES(country_population),
        country_continent = VALUES(country_continent),
        as_rank = VALUES(as_rank),
        cone_asn_count = VALUES(cone_asn_count),
        cone_prefix_count = VALUES(cone_prefix_count),
        cone_address_count = VALUES(cone_address_count),
        customer_degree = VALUES(customer_degree),
        peer_degree = VALUES(peer_degree),
        transit_degree = VALUES(transit_degree),
        provider_degree = VALUES(provider_degree),
        last_update = NOW()
    """

    # SQL for inserting as_relationships
    sql_as_rel = """
    INSERT INTO as_relationships (
        provider_asn,
        customer_asn,
        relationship_type,
        snapshot_date,
        path_count,
        last_update
    )
    VALUES (
        %s, %s, %s, %s, %s, NOW()
    )
    ON DUPLICATE KEY UPDATE
        relationship_type = VALUES(relationship_type),
        path_count = VALUES(path_count),
        last_update = NOW()
    """

    for edge in nodes:
        node = edge.get("node", {})
        # Convert the node's date to first-of-month
        snapshot_date = normalize_date_to_month(node.get("date", date_start))

        # Extract main node fields
        main_asn = node.get("asn")
        main_asnName = node.get("asnName")
        main_rank = node.get("rank")
        org_obj = node.get("organization") or {}
        main_org = org_obj.get("orgName")

        country_obj = node.get("country") or {}
        country_iso = country_obj.get("iso")
        country_name = country_obj.get("name")
        country_capital = country_obj.get("capital")
        country_population = country_obj.get("population")
        country_continent = country_obj.get("continent")

        deg_obj = node.get("asnDegree") or {}
        cust_deg = deg_obj.get("customer")
        peer_deg = deg_obj.get("peer")
        trans_deg = deg_obj.get("transit")
        provider_deg = deg_obj.get("provider")

        cone_obj = node.get("cone") or {}
        cone_asn_count = cone_obj.get("numberAsns")
        cone_prefix_count = cone_obj.get("numberPrefixes")
        cone_address_count = cone_obj.get("numberAddresses")

        # Insert the main node into as_data
        params_main = {
            "asn": main_asn,
            "snapshot_date": snapshot_date,
            "asn_name": main_asnName,
            "org_name": main_org,
            "country_iso": country_iso,
            "country_name": country_name,
            "country_capital": country_capital,
            "country_population": country_population,
            "country_continent": country_continent,
            "as_rank": main_rank,
            "cone_asn_count": cone_asn_count,
            "cone_prefix_count": cone_prefix_count,
            "cone_address_count": cone_address_count,
            "cust_deg": cust_deg,
            "peer_deg": peer_deg,
            "trans_deg": trans_deg,
            "provider_deg": provider_deg
        }
        cursor.execute(sql_as_data_insert, params_main)

        # Insert relationships for link nodes
        links = node.get("asnLinks", {}).get("edges", [])
        for link_edge in links:
            link_node = link_edge.get("node", {})
            relationship = link_node.get("relationship")
            number_paths = link_node.get("numberPaths")
            asn1 = link_node.get("asn1", {})
            link_asn = asn1.get("asn")

            # Insert the link node's as_data
            link_org_obj = asn1.get("organization") or {}
            link_org = link_org_obj.get("orgName")

            link_country_obj = asn1.get("country") or {}
            link_iso = link_country_obj.get("iso")
            link_cname = link_country_obj.get("name")
            link_ccap = link_country_obj.get("capital")
            link_cpop = link_country_obj.get("population")
            link_ccont = link_country_obj.get("continent")

            link_deg = asn1.get("asnDegree") or {}
            link_cust_deg = link_deg.get("customer")
            link_peer_deg = link_deg.get("peer")
            link_trans_deg = link_deg.get("transit")
            link_provider_deg = link_deg.get("provider")

            link_cone = asn1.get("cone") or {}
            link_cone_asn_count = link_cone.get("numberAsns")
            link_cone_prefix_count = link_cone.get("numberPrefixes")
            link_cone_address_count = link_cone.get("numberAddresses")

            link_rank = asn1.get("rank")
            link_asnName = asn1.get("asnName")

            params_link = {
                "asn": link_asn,
                "snapshot_date": snapshot_date,
                "asn_name": link_asnName,
                "org_name": link_org,
                "country_iso": link_iso,
                "country_name": link_cname,
                "country_capital": link_ccap,
                "country_population": link_cpop,
                "country_continent": link_ccont,
                "as_rank": link_rank,
                "cone_asn_count": link_cone_asn_count,
                "cone_prefix_count": link_cone_prefix_count,
                "cone_address_count": link_cone_address_count,
                "cust_deg": link_cust_deg,
                "peer_deg": link_peer_deg,
                "trans_deg": link_trans_deg,
                "provider_deg": link_provider_deg
            }
            cursor.execute(sql_as_data_insert, params_link)
            if relationship == 'provider':
               final_provider_asn = link_asn
               final_customer_asn = main_asn
               final_rel_type = 'provider'
            elif relationship == 'customer':
              # In this case, the main node is the provider and asn1 is the customer.
              final_provider_asn = main_asn
              final_customer_asn = link_asn
              final_rel_type = 'customer'
            elif relationship == 'peer':
               # For peers, we store the relationship as "peer" (or choose one direction consistently)
                final_provider_asn = main_asn
                final_customer_asn = link_asn
                final_rel_type = 'peer'
            else:
               final_provider_asn = link_asn
               final_customer_asn = main_asn
               final_rel_type = relationship or 'unknown'


            # Insert the relationship into as_relationships
            rel_params = (
                final_provider_asn,
                final_customer_asn,
                final_rel_type,
                snapshot_date,
                number_paths
            )
            cursor.execute(sql_as_rel, rel_params)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[INFO] CAIDA data ingestion complete for ASN {asn} from {date_start} to {date_end}")


def dynamic_update(asn, default_start_date="2024-01-01"):
    """
    Attempts to load new monthly data from CAIDA if we have older snapshots.
    Then calls fill_lost_customers_simple to detect newly lost customers.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(snapshot_date) FROM as_data WHERE asn = %s", (asn,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result[0] is None:
        start_date = default_start_date
    else:
        latest_snapshot = result[0]
        next_snapshot = latest_snapshot + relativedelta(months=1)
        start_date = next_snapshot.strftime("%Y-%m-%d")

    current_snapshot = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    if start_date > current_snapshot:
        print("[INFO] No new data to update. Latest snapshot is current.")
        return

    load_caida_data(asn, start_date, current_snapshot)
    # Optionally call fill_lost_customers_simple(asn) here
    fill_lost_customers_simple(asn)

def fill_lost_customers_simple(asn):
    """
    Compare consecutive monthly snapshots in as_relationships for the given ASN
    to identify lost customers. For each lost ASN, look up as_data for org_name
    and cone_asn_count in the last month they were seen.
    Insert the result into lost_customers.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1) Gather all snapshot dates for provider_asn = asn
    cursor.execute("""
        SELECT DISTINCT snapshot_date
        FROM as_relationships
        WHERE provider_asn = %s
        ORDER BY snapshot_date
    """, (asn,))
    date_rows = cursor.fetchall()
    if len(date_rows) < 2:
        print(f"[INFO] Not enough snapshots to compute lost customers for ASN {asn}.")
        cursor.close()
        conn.close()
        return

    months = [row[0] for row in date_rows]

    insert_sql = """
        INSERT INTO lost_customers (
            requestor_asn,
            snapshot_date,
            lost_asn,
            lost_month,
            lost_org,
            lost_cust_cone,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            lost_org = VALUES(lost_org),
            lost_cust_cone = VALUES(lost_cust_cone),
            updated_at=NOW()
    """

    for i in range(len(months) - 1):
        m1 = months[i]   # e.g. 2024-02-01
        m2 = months[i+1] # e.g. 2024-03-01

        # 2) Get the set of customer ASNs for each month
        cursor.execute("""
            SELECT customer_asn
            FROM as_relationships
            WHERE provider_asn = %s
              AND snapshot_date = %s
              AND relationship_type = 'customer'
        """, (asn, m1))
        customers_m1 = set(r[0] for r in cursor.fetchall())

        cursor.execute("""
            SELECT customer_asn
            FROM as_relationships
            WHERE provider_asn = %s
              AND snapshot_date = %s
              AND relationship_type = 'customer'
        """, (asn, m2))
        customers_m2 = set(r[0] for r in cursor.fetchall())

        lost_set = customers_m1 - customers_m2
        if lost_set:
            print(f"[DEBUG] Found {len(lost_set)} lost customers going from {m1} to {m2} for ASN {asn}.")

        for lost_asn in lost_set:
            # Look up org_name and cone_asn_count from as_data for m1
            cursor2 = conn.cursor(dictionary=True)
            cursor2.execute("""
                SELECT org_name, cone_asn_count
                FROM as_data
                WHERE asn = %s
                  AND snapshot_date = %s
                LIMIT 1
            """, (lost_asn, m1))
            row_data = cursor2.fetchone()
            if not row_data:
                # fallback: try the latest snapshot for that asn
                cursor2.execute("""
                    SELECT org_name, cone_asn_count
                    FROM as_data
                    WHERE asn = %s
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                """, (lost_asn,))
                row_data = cursor2.fetchone()
            cursor2.close()

            if row_data:
                lost_org_val = row_data.get("org_name", "Unknown")
                lost_cone_val = row_data.get("cone_asn_count", None)
            else:
                lost_org_val = "Unknown"
                lost_cone_val = None

            cursor.execute(insert_sql, (
                asn,      # requestor_asn
                m1,       # snapshot_date (last month they were a customer)
                lost_asn, # lost_asn
                m2,       # lost_month (first month they're missing)
                lost_org_val,
                lost_cone_val
            ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[INFO] fill_lost_customers_simple completed for ASN {asn}.")

if __name__ == "__main__":
    # Example usage:
    target_asn = "7473"
    dynamic_update(target_asn, "2024-01-01")
    # This loads CAIDA data from 2024-01 to current, inserts new fields in as_data,
    # then calls fill_lost_customers_simple to detect lost events.
