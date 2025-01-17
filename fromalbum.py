from neo4j import GraphDatabase
import csv

class Neo4jHandler:
    def __init__(self, uri, username, password, database):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    @classmethod
    def create_song_album_relationship(cls, tx, song_title, album_title):
        query = (
            "MATCH (s:Song {title: $song_title}) "
            "MATCH (a:Album {title: $album_title}) "
            "MERGE (s)-[:FROM_ALBUM]->(a)"
        )
        tx.run(query, song_title=song_title, album_title=album_title)

    def create_relationships(self, csv_file):
        try:
            with open(csv_file, "r", newline="") as file:
                reader = csv.DictReader(file, delimiter=";")
                # Check if required fields exist in the CSV header
                required_fields = {"Song", "Album"}
                if not required_fields.issubset(reader.fieldnames):
                    raise ValueError("Missing required field(s) in CSV header.")
                for row in reader:
                    song_title = row["Song"]
                    album_title = row["Album"]
                    with self.driver.session(database=self.database) as session:
                        session.write_transaction(self.create_song_album_relationship, song_title, album_title)
        except Exception as e:
            print(f"An error occurred while processing the CSV file: {e}")

def main():
    # Define the connection details
    uri = "bolt://localhost:7687"   # Neo4j Bolt URL
    username = "arun"                # Neo4j username
    password = "98765432"            # Neo4j password
    database = "music"               # Neo4j database name

    # Create an instance of Neo4jHandler
    neo4j_handler = Neo4jHandler(uri, username, password, database)

    # Specify the path to your CSV file
    csv_file = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/songs.csv"

    try:
        # Create relationships between songs and albums
        neo4j_handler.create_relationships(csv_file)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the Neo4j driver
        neo4j_handler.close()

if __name__ == "__main__":
    main()
