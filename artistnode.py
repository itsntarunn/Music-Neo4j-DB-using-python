import csv
from neo4j import GraphDatabase

class Neo4jHandler:
    def __init__(self, uri, username, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(username, password), database=database)

    def close(self):
        self.driver.close()

    @classmethod
    def create_node(cls, uri, username, password, database, label, **properties):
        with GraphDatabase.driver(uri, auth=(username, password), database=database) as driver:
            with driver.session() as session:
                query = f"CREATE (node:{label} $props)"
                session.run(query, props=properties)

def main():
    uri = "bolt://localhost:7687"
    username = "arun"
    password = "98765432"
    database = "music"

    artists_csv_path = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/artists.csv"
    with open(artists_csv_path, "r", newline="") as file:
        reader = csv.reader(file, delimiter=";")
        next(reader)  # Skip header row
        for row in reader:
            try:
                name, born, role, band = row[1:]  # Ignore the first column
                Neo4jHandler.create_node(uri, username, password, database, "Artist", band=band, name=name, born=born, role=role)
                print(f"Node created for artist: {name}")
            except ValueError:
                print(f"Error: Unable to unpack row: {row}")

if __name__ == "__main__":
    main()
