import os
import docker
import re
import sys

def dump_postgres_data(container_name, db_name, user, dump_file='pg_dump.sql'):
    print(f"Dumping data from PostgreSQL container: {container_name}")
    
    client = docker.from_env()
    container = client.containers.get(container_name)
    
    if container.status != 'running':
        print(f"Error: Container {container_name} is not running.")
        sys.exit(1)
    
    # Dump PostgreSQL data
    command = f'docker exec -t {container_name} pg_dump -U {user} -d {db_name} > {dump_file}'
    os.system(command)

def convert_pg_to_mysql(pg_dump_file='pg_dump.sql', mysql_dump_file='mysql_dump.sql'):
    print("Converting PostgreSQL dump to MySQL format")
    
    with open(pg_dump_file, 'r') as file:
        content = file.read()
    
    # Example conversions: these are basic and might need customization
    content = re.sub(r'(\bSERIAL\b)', 'INT AUTO_INCREMENT', content)
    content = re.sub(r'(\bBOOLEAN\b)', 'TINYINT(1)', content)
    content = re.sub(r'(\b\w+\s+\bUUID\b)', 'CHAR(36)', content)
    content = re.sub(r'(\bIF NOT EXISTS\b)', '', content)  # MySQL doesn't use this in CREATE TABLE
    
    # Write the converted content to a new file
    with open(mysql_dump_file, 'w') as file:
        file.write(content)
    
    print(f"Conversion completed. MySQL dump saved to {mysql_dump_file}")

def create_mysql_container(container_name, root_password, db_name):
    print(f"Creating MySQL container: {container_name}")
    
    client = docker.from_env()
    container = client.containers.run(
        'mysql:latest',
        name=container_name,
        environment={"MYSQL_ROOT_PASSWORD": root_password, "MYSQL_DATABASE": db_name},
        detach=True
    )
    return container

def import_mysql_data(container_name, root_password, db_name, mysql_dump_file='mysql_dump.sql'):
    print(f"Importing data into MySQL container: {container_name}")
    
    if not os.path.exists(mysql_dump_file):
        print(f"Error: File {mysql_dump_file} not found.")
        sys.exit(1)
    
    command = f'docker exec -i {container_name} mysql -u root -p{root_password} {db_name} < {mysql_dump_file}'
    os.system(command)

def migrate_postgres_to_mysql(postgres_container, mysql_container, postgres_db, postgres_user, mysql_db, mysql_root_password):
    dump_postgres_data(postgres_container, postgres_db, postgres_user)
    convert_pg_to_mysql()
    mysql_cont = create_mysql_container(mysql_container, mysql_root_password, mysql_db)
    import_mysql_data(mysql_container, mysql_root_password, mysql_db)
    print(f"Migration from {postgres_container} to {mysql_container} completed.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate PostgreSQL container to MySQL container.")
    parser.add_argument("--source", required=True, help="Source PostgreSQL container name")
    parser.add_argument("--target", required=True, help="Target MySQL container name")
    parser.add_argument("--pgdb", required=True, help="PostgreSQL database name")
    parser.add_argument("--pguser", required=True, help="PostgreSQL username")
    parser.add_argument("--mysqldb", required=True, help="MySQL database name")
    parser.add_argument("--mysqlroot", required=True, help="MySQL root password")

    args = parser.parse_args()

    migrate_postgres_to_mysql(
        args.source, 
        args.target, 
        args.pgdb, 
        args.pguser, 
        args.mysqldb, 
        args.mysqlroot
    )
