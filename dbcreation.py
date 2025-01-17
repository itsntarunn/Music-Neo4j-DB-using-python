from neo4j import GraphDatabase

# Connect to Neo4j instance
uri = "bolt://localhost:7687"
username = "arun"
password = "98765432"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Create a new database by executing Cypher query
with driver.session() as session:
    create_db_query = "CREATE DATABASE music"
    session.run(create_db_query)

# Close the Neo4j driver
driver.close()
