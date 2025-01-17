from neo4j import GraphDatabase
import csv

# Define the connection details
uri = "bolt://localhost:7687"   # Neo4j Bolt URL
username = "neo4j"               # Neo4j username
password = "12345678"            # Neo4j password

# Function to create a relationship between a song and an artist
def create_relationship(tx, song_title, artist_name, relationship_type):
    query = (
        "MATCH (s:Song {title: $song_title}), (a:Artist {name: $artist_name}) "
        "MERGE (s)-[r:{relationship_type}]->(a)"
    )
    tx.run(query, song_title=song_title, artist_name=artist_name, relationship_type=relationship_type)

def main():
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Read data from songs.csv and create relationships
    with open("C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/songs.csv", "r", newline="") as file:
        reader = csv.DictReader(file, delimiter=";")
        for row in reader:
            song_title = row["Song"]
            artist_name = row["DJ"]
            relationship_type = "DJ"  # or any other appropriate relationship type
            if artist_name:  # Check if the DJ name is not empty
                with driver.session() as session:
                    session.write_transaction(create_relationship, song_title, artist_name, relationship_type)

    # Close the driver
    driver.close()

if __name__ == "__main__":
    main()
