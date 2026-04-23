"""
Quick demo: fires 3 test entries through the audit logger.
  - Test 1: normal successful query
  - Test 2: sensitive param gets redacted
  - Test 3: simulated error
"""
import sys
sys.path.insert(0, "mcp")

from audit_logger import timed_query

print("\n--- Test 1: normal query ---")
with timed_query("get_delivery_performance", {"group_by": "state", "limit": 10}, "SELECT state, AVG(days_to_deliver) FROM fct_orders GROUP BY state LIMIT 10") as ctx:
    ctx["row_count"] = 10
print("✓ logged 10 rows")

print("\n--- Test 2: sensitive param (customer_id should be redacted in log) ---")
with timed_query("get_customer_segments", {"customer_id": "abc123", "state": "SP"}, "SELECT * FROM dim_customers WHERE state = ?") as ctx:
    ctx["row_count"] = 5
print("✓ logged — customer_id appears as <redacted> in log")

print("\n--- Test 3: simulated error ---")
try:
    with timed_query("run_sql", {"query_preview": "DROP TABLE orders"}, "DROP TABLE orders") as ctx:
        raise ValueError("Only SELECT / WITH queries are allowed.")
except ValueError as e:
    print(f"✓ error caught and logged: {e}")

print("\n--- Done. Check logs/mcp_audit.log for the 3 new entries ---")
