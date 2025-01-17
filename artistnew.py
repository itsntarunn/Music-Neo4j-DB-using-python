import pandas as pd
from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, username, password, database):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = None

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password), database=self.database)
            print("Connected to Neo4j")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")

    def close(self):
        if self.driver:
            self.driver.close()
            print("Connection to Neo4j closed")

class ArtistNodeCreator:
    @classmethod
    def create_artist_nodes_from_csv(cls, csv_path, connector):
        try:
            df = pd.read_csv(csv_path, delimiter=';')
            print("CSV file loaded successfully")
        except Exception as e:
            print(f"Failed to load CSV file: {e}")
            return

        with connector.driver.session() as session:
            tx = session.begin_transaction()
            for index, row in df.iterrows():
                name = row['name']
                born = row['born']
                role = row['role']
                band = row['Band']
                # Create artist node
                create_artist_query = """
                MERGE (a:Artist {name: $name, born: $born, role: $role, band: $band})
                """
                tx.run(create_artist_query, name=name, born=born, role=role, band=band)
                print(f"Artist node created: {name} - {born} - {role} - {band}")
            tx.commit()

def main():
    # Neo4j connection details
    uri = "bolt://localhost:7687"
    username = "arun"
    password = "98765432"
    database = "music"

    # Specify the CSV file path
    csv_path = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/artists.csv"

    # Create Neo4jConnector instance
    connector = Neo4jConnector(uri, username, password, database)

    # Connect to Neo4j
    connector.connect()

    # Create artist nodes from CSV
    ArtistNodeCreator.create_artist_nodes_from_csv(csv_path, connector)

    # Close Neo4j connection
    connector.close()

if __name__ == "__main__":
    main()
