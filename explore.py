import psycopg2
import subprocess
import json

CONTAINER_NAME = "postgres_migration"

def run_docker_command(command):
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Docker command failed: {result.stderr}")
    return result.stdout.strip()

def get_container_info():
    inspect_output = run_docker_command(["docker", "inspect", CONTAINER_NAME])
    return json.loads(inspect_output)[0]

def get_postgres_connection(database='postgres'):
    container_info = get_container_info()
    
    port_mappings = container_info['NetworkSettings']['Ports']['5432/tcp']
    host_port = port_mappings[0]['HostPort'] if port_mappings else '5432'
    
    env_vars = {env.split('=')[0]: env.split('=')[1] for env in container_info['Config']['Env']}
    
    return psycopg2.connect(
        host='localhost',
        port=host_port,
        user=env_vars.get('POSTGRES_USER', 'postgres'),
        password=env_vars.get('POSTGRES_PASSWORD', 'postgres'),
        database=database
    )

def list_databases():
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [row[0] for row in cur.fetchall()]
        print(f"Available databases in the container '{CONTAINER_NAME}':")
        for db in databases:
            print(f"- {db}")
        return databases
    finally:
        cur.close()
        conn.close()

def explore_user_tables(db_name):
    conn = get_postgres_connection(db_name)
    cur = conn.cursor()
    
    try:
        print(f"\nExploring user tables in database: {db_name}")

        # List tables in public schema
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        if tables:
            print("\nYour tables:")
            for table in tables:
                print(f"\nTable: {table}")
                
                # Get column information for each table
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = %s 
                    ORDER BY ordinal_position;
                """, (table,))
                columns = cur.fetchall()
                print("Columns:")
                for column in columns:
                    print(f"  - {column[0]} ({column[1]})")
        else:
            print("No user-created tables found in the public schema.")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    try:
        available_dbs = list_databases()
        db_to_explore = input("Enter the name of the database you want to explore: ").strip()
        
        if db_to_explore in available_dbs:
            explore_user_tables(db_to_explore)
        else:
            print(f"Database '{db_to_explore}' not found. Please check the name and try again.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")