import falkordb
import textwrap
import time

def create_despegar_data_graph(g):
    """
    Creates the nodes and relationships for the Despegar Data team graph.
    """
    # Clean up previous data for idempotency
    print("Cleaning up existing graph data...")
    g.query("MATCH (n) DETACH DELETE n")

    # 1. Create Business Units and Owners
    print("Creating BusinessUnit and Owner nodes...")
    g.query("""
    CREATE (:BusinessUnit {name: 'Commercial'}),
           (:BusinessUnit {name: 'Finance'}),
           (:BusinessUnit {name: 'Customer Experience'}),
           (:Owner {name: 'Data Engineering'}),
           (:Owner {name: 'BI Team'}),
           (:Owner {name: 'Finance Analytics'})
    """)

    # 2. Create Models and associate them with Business Units
    print("Creating Model nodes...")
    g.query("""
    MATCH (bu_c:BusinessUnit {name:'Commercial'}), (bu_f:BusinessUnit {name:'Finance'})
    CREATE (:Model {name:'Client'})-[:BELONGS_TO]->(bu_c),
           (:Model {name:'Sales'})-[:BELONGS_TO]->(bu_c),
           (:Model {name:'Finance'})-[:BELONGS_TO]->(bu_f)
    """)

    # 3. Create Tables and Columns, link to Models and Owners
    print("Creating Table and Column nodes...")
    g.query("""
    MATCH (m:Model {name:'Client'}), (owner:Owner {name:'Data Engineering'})
    CREATE (t:Table {name:'users', description:'Master table for user data', creation_date: '2023-01-15'})-[:OWNED_BY]->(owner),
           (t)-[:HAS_COLUMN]->(:Column {name:'user_id'}),
           (t)-[:HAS_COLUMN]->(:Column {name:'name'}),
           (t)-[:HAS_COLUMN]->(:Column {name:'email'}),
           (m)-[:USES]->(t)
    """)
    g.query("""
    MATCH (m:Model {name:'Sales'}), (owner:Owner {name:'Data Engineering'})
    CREATE (t1:Table {name:'transactions', description:'Raw sales transactions', creation_date: '2023-02-01'})-[:OWNED_BY]->(owner),
           (t1)-[:HAS_COLUMN]->(:Column {name:'transaction_id'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'user_id'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'product_id'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'amount'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'date'}),
           (t2:Table {name:'products', description:'Product catalog', creation_date: '2022-11-20'})-[:OWNED_BY]->(owner),
           (t2)-[:HAS_COLUMN]->(:Column {name:'product_id'}),
           (t2)-[:HAS_COLUMN]->(:Column {name:'name'}),
           (t2)-[:HAS_COLUMN]->(:Column {name:'price'}),
           (m)-[:USES]->(t1), (m)-[:USES]->(t2)
    """)
    g.query("""
    MATCH (m:Model {name:'Finance'}), (owner:Owner {name:'Finance Analytics'})
    CREATE (t1:Table {name:'invoices', description:'Customer invoices', creation_date: '2023-02-02'})-[:OWNED_BY]->(owner),
           (t1)-[:HAS_COLUMN]->(:Column {name:'invoice_id'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'transaction_id'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'amount'}),
           (t1)-[:HAS_COLUMN]->(:Column {name:'status'}),
           (t2:Table {name:'payments', description:'Payment records', creation_date: '2023-02-03'})-[:OWNED_BY]->(owner),
           (t2)-[:HAS_COLUMN]->(:Column {name:'payment_id'}),
           (t2)-[:HAS_COLUMN]->(:Column {name:'invoice_id'}),
           (t2)-[:HAS_COLUMN]->(:Column {name:'amount'}),
           (t2)-[:HAS_COLUMN]->(:Column {name:'method'}),
           (m)-[:USES]->(t1), (m)-[:USES]->(t2)
    """)

    # 4. Create Jobs and their relationships with Tables and Owners
    print("Creating Job nodes and relationships...")
    g.query("""
    MATCH (t_transactions:Table {name:'transactions'}), (t_invoices:Table {name:'invoices'}), (owner:Owner {name:'Data Engineering'})
    CREATE (j1:Job {name:'job_generate_invoices', schedule: 'daily@01:00', cost: 5})-[:OWNED_BY]->(owner),
           (j1)-[:CONSUMES]->(t_transactions),
           (j1)-[:PRODUCES]->(t_invoices)
    """)
    g.query("""
    MATCH (t_transactions:Table {name:'transactions'}), (owner:Owner {name:'Data Engineering'})
    CREATE (t_daily_sales:Table {name:'daily_sales_summary', description:'Aggregated daily sales'})-[:OWNED_BY]->(owner),
           (j2:Job {name:'job_process_sales', schedule: 'daily@03:00', cost: 8})-[:OWNED_BY]->(owner),
           (j2)-[:CONSUMES]->(t_transactions),
           (j2)-[:PRODUCES]->(t_daily_sales)
    """)

    # 5. Create KPIs and their relationships with Columns and Owners
    print("Creating KPI nodes and relationships...")
    # Create KPI nodes
    g.query("""CREATE (:KPI {name:'total_sales', description:'Sum of all transaction amounts'})""")
    g.query("""CREATE (:KPI {name:'active_users', description:'Count of unique users with transactions'})""")
    # Create KPI relationships
    g.query("""MATCH (k:KPI {name:'total_sales'}), (c:Column {name:'amount'})<-[:HAS_COLUMN]-(:Table {name:'transactions'}) CREATE (k)-[:CALCULATED_FROM]->(c)""")
    g.query("""MATCH (k:KPI {name:'total_sales'}), (o:Owner {name:'BI Team'}) CREATE (k)-[:OWNED_BY]->(o)""")
    g.query("""MATCH (k:KPI {name:'active_users'}), (c:Column {name:'user_id'})<-[:HAS_COLUMN]-(:Table {name:'users'}) CREATE (k)-[:CALCULATED_FROM]->(c)""")
    g.query("""MATCH (k:KPI {name:'active_users'}), (c:Column {name:'user_id'})<-[:HAS_COLUMN]-(:Table {name:'transactions'}) CREATE (k)-[:CALCULATED_FROM]->(c)""")
    g.query("""MATCH (k:KPI {name:'active_users'}), (o:Owner {name:'BI Team'}) CREATE (k)-[:OWNED_BY]->(o)""")

    # 6. Create Dashboards and their relationships with Owners
    print("Creating Dashboard nodes and relationships...")
    # Create Dashboard nodes
    g.query("""CREATE (:Dashboard {name:'Sales Dashboard', last_updated: '2024-06-25'})""")
    g.query("""CREATE (:Dashboard {name:'Finance Dashboard', last_updated: '2024-06-25'})""")
    # Create Dashboard relationships
    g.query("""MATCH (d:Dashboard {name:'Sales Dashboard'}), (k:KPI {name:'total_sales'}) CREATE (d)-[:DISPLAYS]->(k)""")
    g.query("""MATCH (d:Dashboard {name:'Sales Dashboard'}), (o:Owner {name:'BI Team'}) CREATE (d)-[:OWNED_BY]->(o)""")
    g.query("""MATCH (d:Dashboard {name:'Finance Dashboard'}), (t:Table {name:'invoices'}) CREATE (d)-[:USES]->(t)""")
    g.query("""MATCH (d:Dashboard {name:'Finance Dashboard'}), (o:Owner {name:'Finance Analytics'}) CREATE (d)-[:OWNED_BY]->(o)""")

    print("\nGraph for Despegar Data team created successfully.")
    print("You can view the graph at http://localhost:3000 if you are running FalkorDB with the UI.")

def run_example_queries(g):
    """
    Runs a series of example queries against the graph and prints the results.
    """
    print("\n--- Running some example queries ---")

    # 1. What tables are used by the 'Sales' model?
    print("\n1. What tables are used by the 'Sales' model?")
    res = g.query("MATCH (:Model {name:'Sales'})-[:USES]->(t:Table) RETURN t.name, t.description")
    for row in res.result_set:
        print(f"- {row[0]}: {row[1]}")

    # 2. What job produces the 'invoices' table?
    print("\n2. What job produces the 'invoices' table?")
    res = g.query("MATCH (j:Job)-[:PRODUCES]->(:Table {name:'invoices'}) RETURN j.name, j.schedule, j.cost")
    for row in res.result_set:
        print(f"- {row[0]} (schedule: {row[1]}, cost: {row[2]})")

    # 3. What KPIs are in the 'Sales Dashboard'?
    print("\n3. What KPIs are in the 'Sales Dashboard'?")
    res = g.query("MATCH (:Dashboard {name:'Sales Dashboard'})-[:DISPLAYS]->(k:KPI) RETURN k.name, k.description")
    for row in res.result_set:
        print(f"- {row[0]}: {row[1]}")

    # 4. Impact analysis: If column 'transactions.amount' changes, what downstream assets are affected?
    print("\n4. Impact analysis: If column 'transactions.amount' changes, what downstream assets are affected?")
    res = g.query("""
    MATCH (:Table {name:'transactions'})-[:HAS_COLUMN]->(:Column {name:'amount'})<-[:CALCULATED_FROM]-(:KPI)<-[:DISPLAYS]-(d:Dashboard)
    RETURN d.name as affected_dashboard
    """)
    for row in res.result_set:
        print(f"- Dashboard '{row[0]}' is affected.")

    # 5. Lineage: What is the full upstream lineage for the 'total_sales' KPI?
    print("\n5. Lineage: What is the full upstream lineage for the 'total_sales' KPI?")
    query = """
    MATCH p=(k:KPI {name:'total_sales'})<-[*]-(n)
    RETURN p
    """
    res = g.query(textwrap.dedent(query))
    print(f"Found {len(res.result_set)} upstream paths for the 'total_sales' KPI.")

    # 6. Who owns the 'Finance Dashboard'?
    print("\n6. Who owns the 'Finance Dashboard'?")
    res = g.query("MATCH (:Dashboard {name:'Finance Dashboard'})-[:OWNED_BY]->(o:Owner) RETURN o.name")
    for row in res.result_set:
        print(f"- {row[0]}")

    # 7. Which assets belong to the 'Commercial' Business Unit?
    print("\n7. Which assets belong to the 'Commercial' Business Unit?")
    res = g.query("""
    MATCH (:BusinessUnit {name:'Commercial'})<-[:BELONGS_TO]-(m:Model)-[:USES]->(t:Table)
    RETURN m.name as model, t.name as table
    """)
    for row in res.result_set:
        print(f"- Table '{row[1]}' is used by Model '{row[0]}'")

    print("\nQueries complete. You can now explore the enhanced graph visually in the FalkorDB browser UI (http://localhost:3000).")
    print("Try running 'MATCH p=(n)-[r]->(m) RETURN p' to see the whole graph.")


def main():
    """
    Main function to connect to FalkorDB and run the POC.
    """
    retries = 5
    db = None
    while retries > 0:
        try:
            # Connect to FalkorDB using the service name 'falkordb'
            db = falkordb.FalkorDB(host='falkordb', port=6379)
            # Check if the connection is alive
            db.select_graph('DespegarData')
            print("Successfully connected to FalkorDB.")
            break
        except Exception as e:
            retries -= 1
            print(f"Could not connect to FalkorDB: {e}. Retrying in 5 seconds... ({retries} retries left)")
            time.sleep(5)

    if not db:
        print("Failed to connect to FalkorDB after several attempts. Exiting.")
        return

    try:
        # Select the 'DespegarData' graph
        g = db.select_graph('DespegarData')

        create_despegar_data_graph(g)
        run_example_queries(g)

    except Exception as e:
        print(f"An error occurred during graph creation or querying: {e}")

if __name__ == "__main__":
    main() 