from neo4j import GraphDatabase
import csv

# Define the connection details
uri = "bolt://localhost:7687"   # Neo4j Bolt URL
username = "arun"                # Neo4j username
password = "98765432"            # Neo4j password
database = "music"               # Neo4j database name

# Define the Album class
class Album:
    def __init__(self, band, title, released, genre):
        self.band = band
        self.title = title
        self.released = released
        self.genre = genre
    
    @classmethod
    def from_csv_row(cls, csv_row):
        try:
            band, title, released, genre = csv_row
            return cls(band, title, released, genre)
        except ValueError:
            print(f"Error: Unable to unpack row: {csv_row}")
            return None

# Function to create or merge a node for each album
def create_album_node(tx, album):
    query = (
        "MERGE (:Album {band: $band, title: $title, released: $released, genre: $genre})"
    )
    tx.run(query, **album.__dict__)

def main():
    # Connect to Neo4j and specify the database
    driver = GraphDatabase.driver(uri, auth=(username, password), database=database)

    # Read data from albums.csv and create nodes
    with open("C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/album.csv", "r", newline="") as file:
        reader = csv.reader(file, delimiter=";")
        # Skip the header row
        next(reader)
        for row in reader:
            try:
                album = Album.from_csv_row(row)
                if album:
                    with driver.session() as session:
                        session.write_transaction(create_album_node, album)
            except ValueError:
                print(f"Error: Unable to unpack row: {row}")

    # Close the driver
    driver.close()

if __name__ == "__main__":
    main()
