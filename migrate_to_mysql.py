import subprocess
import os
import time
import re
import sys

def run_command(command, shell=False):
    print(f"Running command: {command}")
    result = subprocess.run(command, shell=shell, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Command failed. Error output: {result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout.strip()

def dump_postgres_db(pg_container, db_name, output_file):
    print(f"Dumping PostgreSQL database '{db_name}'...")
    cmd = f"docker exec {pg_container} pg_dump -U postgres --no-owner --no-acl {db_name}"
    try:
        with open(output_file, 'w') as outfile:
            subprocess.run(cmd.split(), stdout=outfile, check=True)
        print(f"PostgreSQL dump completed: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error during PostgreSQL dump: {e}")
        print(f"Error output: {e.stderr}")
        raise

def convert_to_mysql(input_file, output_file):
    print("Converting PostgreSQL dump to MySQL format...")
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                if line.startswith('SET') or line.startswith('SELECT pg_catalog.set_config'):
                    continue

                # Skip sequence settings
                if line.startswith("SELECT pg_catalog.setval("):
                    continue

                # Basic replacements
                line = re.sub(r'::(\w+)', '', line)  # Remove type casts
                line = line.replace('SERIAL', 'INT AUTO_INCREMENT')
                line = line.replace('NOW()', 'CURRENT_TIMESTAMP')
                line = line.replace('BOOLEAN', 'TINYINT(1)')
                line = line.replace('RETURNING', '')
                line = line.replace('IF NOT EXISTS', '')
                line = line.replace('ALTER TABLE ONLY', 'ALTER TABLE')
                line = line.replace('WITH TIME ZONE', '')
                line = line.replace('WITHOUT TIME ZONE', '')
                line = line.replace('COMMENT ON EXTENSION', '-- COMMENT ON EXTENSION')

                # Remove references to 'public' schema
                line = re.sub(r'public\.', '', line)
                line = line.replace('CREATE SCHEMA public;', '')
                line = line.replace('ALTER SCHEMA public OWNER TO postgres;', '')

                # Skip any lines that create or alter sequences
                if 'CREATE SEQUENCE' in line or 'ALTER SEQUENCE' in line or 'DROP SEQUENCE' in line:
                    continue

                # Remove any remaining sequence-related clauses
                line = re.sub(r'(?i)CREATE SEQUENCE\s+\w+\s*(START\s*WITH\s*\d+\s*INCREMENT\s*BY\s*\d+\s*)?', '', line)
                line = re.sub(r'(?i)ALTER TABLE\s+\w+\s+ALTER COLUMN\s+\w+\s+SET DEFAULT\s+nextval\(\s*\'\w+\'\s*\)', '', line)
                line = re.sub(r'(?i)WITH\s+\d+\s*INCREMENT\s*BY\s*\d+', '', line)
                line = re.sub(r'(?i)NO\s*MINVALUE', '', line)
                line = re.sub(r'(?i)NO\s*MAXVALUE', '', line)
                line = re.sub(r'(?i)CACHE\s*\d+', '', line)
                line = re.sub(r'(?i)AS\s*integer', '', line)

                # Clean up double spaces resulting from removals
                line = re.sub(r'\s+', ' ', line).strip()

                # Write cleaned line to the output file
                if line:  # Only write non-empty lines
                    outfile.write(line + '\n')
        print(f"Conversion completed: {output_file}")
    except Exception as e:
        print(f"Error during conversion: {e}")
        raise


def create_mysql_container(container_name, root_password):
    print(f"Creating MySQL container '{container_name}'...")
    cmd = f"docker run --name {container_name} -e MYSQL_ROOT_PASSWORD={root_password} -d mysql:latest"
    try:
        container_id = run_command(cmd, shell=True)
        print(f"MySQL container created: {container_id}")
        return container_id
    except Exception as e:
        print(f"Error creating MySQL container: {e}")
        raise

def wait_for_mysql(container_name, root_password):
    print("Waiting for MySQL to be ready...")
    max_attempts = 30
    attempts = 0
    while attempts < max_attempts:
        try:
            cmd = f"docker exec {container_name} mysqladmin -u root -p{root_password} ping --silent"
            status = run_command(cmd, shell=True)
            if 'mysqld is alive' in status:
                print("MySQL is ready.")
                return
        except Exception as e:
            print(f"MySQL is not ready yet, retrying... Attempt {attempts + 1}/{max_attempts}")
        attempts += 1
        time.sleep(2)
    raise Exception("MySQL failed to become ready in time")

def import_to_mysql(container_name, db_name, root_password, input_file):
    print(f"Importing data into MySQL database '{db_name}'...")
    create_db_cmd = f"docker exec {container_name} mysql -u root -p{root_password} -e \"CREATE DATABASE IF NOT EXISTS {db_name};\""
    try:
        run_command(create_db_cmd, shell=True)
        
        print("Contents of MySQL dump file:")
        with open(input_file, 'r') as f:
            print(f.read())
        
        # Adjusted import command
        import_cmd = f"docker exec -i {container_name} mysql -u root -p{root_password} {db_name} < {input_file}"
        result = subprocess.run(import_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Import failed. Error: {result.stderr}")
            raise Exception("Import failed")
        print("Import completed.")
    except Exception as e:
        print(f"Error during MySQL import: {e}")
        raise

# Configuration
PG_CONTAINER = "postgres_migration"
MYSQL_CONTAINER = "migration_mysql"
MYSQL_ROOT_PASSWORD = "hamza"
DB_NAME = input("Enter the name of the database you want to migrate: ").strip()

if not DB_NAME:
    print("Error: Database name cannot be empty.")
    sys.exit(1)

# Main migration process
try:
    pg_dump_file = f"{DB_NAME}_pg_dump.sql"
    mysql_dump_file = f"{DB_NAME}_mysql_dump.sql"

    # Step 1: Dump PostgreSQL database
    dump_postgres_db(PG_CONTAINER, DB_NAME, pg_dump_file)

    # Step 2: Convert to MySQL format
    convert_to_mysql(pg_dump_file, mysql_dump_file)

    # Step 3: Create MySQL container
    create_mysql_container(MYSQL_CONTAINER, MYSQL_ROOT_PASSWORD)

    # Step 4: Wait for MySQL to be ready
    wait_for_mysql(MYSQL_CONTAINER, MYSQL_ROOT_PASSWORD)

    # Step 5: Import into MySQL
    import_to_mysql(MYSQL_CONTAINER, DB_NAME, MYSQL_ROOT_PASSWORD, mysql_dump_file)

    print(f"Migration completed successfully. Your data is now in the MySQL container '{MYSQL_CONTAINER}'.")
    print(f"You can connect to it using: docker exec -it {MYSQL_CONTAINER} mysql -u root -p{MYSQL_ROOT_PASSWORD}")

except Exception as e:
    print(f"An error occurred during migration: {str(e)}")

finally:
    # Clean up dump files
    if os.path.exists(pg_dump_file):
        os.remove(pg_dump_file)
    if os.path.exists(mysql_dump_file):
        os.remove(mysql_dump_file)
