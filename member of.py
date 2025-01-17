import csv
from neo4j import GraphDatabase

class Neo4jConnector:
    @classmethod
    def create_relationship(cls, uri, username, password, database, artist_name, band_name):
        driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=(username, password), database=database)
            with driver.session() as session:
                create_relationship_query = (
                    "MERGE (a:Artist {name: $artist_name}) "
                    "MERGE (b:Band {name: $band_name}) "
                    "MERGE (a)-[:MEMBER_OF]->(b)"
                )
                session.run(create_relationship_query, artist_name=artist_name, band_name=band_name)
                print(f"Relationship created: {artist_name} is MEMBER_OF {band_name}")
        except Exception as e:
            print(f"Failed to create relationship: {e}")
        finally:
            if driver:
                driver.close()

def main():
    uri = "bolt://localhost:7687"
    username = "arun"
    password = "98765432"
    database = "music"

    # Open the CSV file containing artist-band relationships
    csv_file = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/artists.csv"
    with open(csv_file, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file, delimiter=";")
        next(reader)  # Skip header
        for row in reader:
            artist_name = row["name"]
            band_name = row["Band"]
            Neo4jConnector.create_relationship(uri, username, password, database, artist_name, band_name)

if __name__ == "__main__":
    main()
