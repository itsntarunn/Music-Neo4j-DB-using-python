from neo4j import GraphDatabase
import csv

# Define the connection details
uri = "bolt://localhost:7687"   # Neo4j Bolt URL
username = "neo4j"               # Neo4j username
password = "12345678"            # Neo4j password

class Neo4jImporter:
    def __init__(self, uri, username, password):
        self.uri = uri
        self.username = username
        self.password = password

    def create_song_dj_relationship(self, tx, song_title, dj_name):
        query = (
            "MATCH (s:Song {title: $song_title}), (d:Artist {name: $dj_name}) "
            "MERGE (s)-[:DJ]->(d)"
        )
        tx.run(query, song_title=song_title, dj_name=dj_name)
        print(f"Relationship created: Song '{song_title}' - DJ '{dj_name}'")

    def import_data(self, songs_file):
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        with open(songs_file, "r", newline="") as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                song_title = row["Song"]
                dj_name = row["DJ"]
                if dj_name:  # Check if the DJ name is not empty
                    with driver.session() as session:
                        session.write_transaction(self.create_song_dj_relationship, song_title, dj_name)
        driver.close()

def main():
    importer = Neo4jImporter(uri, username, password)
    importer.import_data("C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/songs.csv")

if __name__ == "__main__":
    main()
