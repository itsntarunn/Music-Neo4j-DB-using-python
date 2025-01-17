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

    def create_relationship(self, tx, song_title, drummer_name):
        query = (
            "MERGE (s:Song {title: $song_title}) "
            "MERGE (d:Drummer {name: $drummer_name}) "
            "MERGE (s)-[:DRUMMER]->(d)"
        )
        tx.run(query, song_title=song_title, drummer_name=drummer_name)

    def create_relationships(self, csv_file):
        try:
            with open(csv_file, "r", newline="", encoding="utf-8-sig") as file:
                reader = csv.DictReader(file, delimiter=";")
                for row in reader:
                    song_title = row["Song"]
                    drummer_name = row["Drummer"]
                    with self.driver.session(database=self.database) as session:
                        session.write_transaction(self.create_relationship, song_title, drummer_name)
                        print(f"Relationship created: Song '{song_title}' - Drummer '{drummer_name}'")
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
        # Create relationships in Neo4j from the CSV file
        neo4j_handler.create_relationships(csv_file)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the Neo4j driver
        neo4j_handler.close()

if __name__ == "__main__":
    main()
