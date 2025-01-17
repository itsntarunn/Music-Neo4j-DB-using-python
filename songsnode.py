from neo4j import GraphDatabase
import csv

# Define the connection details
uri = "bolt://localhost:7687"   # Neo4j Bolt URL
username = "arun"                # Neo4j username
password = "98765432"            # Neo4j password
database = "music"               # Neo4j database name

# Define the Song class
class Song:
    def __init__(self, title, released, genre, band, album):
        self.title = title
        self.released = released
        self.genre = genre
        self.band = band
        self.album = album
   
    @classmethod
    def from_csv_row(cls, csv_row):
        try:
            title, released, genre, band, album, *_ = csv_row # Take only the first 5 elements
            return cls(title, released, genre, band, album)
        except ValueError:
            print(f"Error: Unable to unpack row: {csv_row}")
            return None

# Function to create or merge a node for each song
def create_song_node(tx, title, released, genre, band, album):
    query = (
        "MERGE (:Song {title: $title, released: $released, genre: $genre, band: $band, album: $album})"
    )
    tx.run(query, title=title, released=released, genre=genre, band=band, album=album)

def main():
    # Connect to Neo4j
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        print("Connection to Neo4j successful.")
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        return

    # Read data from songs.csv and create nodes
    songs_csv_path = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/songs.csv"
    print(f"Reading data from {songs_csv_path}...")
    try:
        with open(songs_csv_path, "r", newline="") as file:
            reader = csv.reader(file, delimiter=";")
            # Skip the header row
            next(reader)
            for row in reader:
                print(f"Processing row: {row}")
                try:
                    with driver.session(database=database) as session:
                        print("Session created successfully.")
                        song = Song.from_csv_row(row)
                        if song:
                            print(f"Inserting node for song: {song.title}")
                            session.write_transaction(create_song_node, song.title, song.released, song.genre, song.band, song.album)
                except Exception as e:
                    print(f"Error inserting node for song: {e}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")

    # Close the driver
    print("Closing Neo4j connection...")
    driver.close()

if __name__ == "__main__":
    main()
