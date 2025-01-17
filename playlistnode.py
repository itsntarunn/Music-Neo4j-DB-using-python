from neo4j import GraphDatabase
import csv

# Define the connection details
uri = "bolt://localhost:7687"   # Neo4j Bolt URL
username = "arun"                # Neo4j username
password = "98765432"            # Neo4j password
database = "music"               # Neo4j database name

class Playlist:
    @classmethod
    def create(cls, tx, name):
        query = "MERGE (:Playlist {name: $name})"
        tx.run(query, name=name)

def main():
    # Connect to Neo4j
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        print("Connection to Neo4j successful.")
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        return

    # Read data from playlists.csv and create playlists
    with open("C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/playlist.csv", "r", newline="") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            playlist_name = row[0]
            with driver.session(database=database) as session:
                session.write_transaction(Playlist.create, playlist_name)

    # Close the driver
    driver.close()

if __name__ == "__main__":
    main()
