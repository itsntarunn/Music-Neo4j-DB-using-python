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

    @classmethod
    def create_nodes_and_relationships(cls, csv_path, uri, username, password, database):
        connector = cls(uri, username, password, database)
        connector.connect()

        try:
            df = pd.read_csv(csv_path, delimiter=';')
            print("CSV file loaded successfully")
        except Exception as e:
            print(f"Failed to load CSV file: {e}")
            return

        with connector.driver.session() as session:
            for index, row in df.iterrows():
                song = row['Song']
                playlist = row['Playlist']
                album = row['Album']
                lyricist = row.get('Lyricist')
                bassist = row.get('Bassist')
                dj = row.get('DJ')
                drummer = row.get('Drummer')
                band = row.get('Band')  # Assuming 'Band' is the column containing band names
                
                # Create song node and map to playlist, album, and band
                create_and_map_query = """
                MERGE (s:Song {name: $song})
                MERGE (p:Playlist {name: $playlist})
                MERGE (a:Album {name: $album})
                MERGE (b:Band {name: $band})
                MERGE (s)-[:BELONGS_TO]->(p)
                MERGE (s)-[:IN_ALBUM]->(a)
                MERGE (s)-[:BY_BAND]->(b)
                """
                session.run(create_and_map_query, song=song, playlist=playlist, album=album, band=band)
                print(f"Song node created and mapped to playlist, album, and band: {song} - {playlist} - {album} - {band}")
                
                # Map other attributes (lyricist, bassist, dj, drummer) to song
                if lyricist:
                    map_lyricist_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (l:Artist {name: $lyricist})
                    MERGE (s)-[:LYRICIST]->(l)
                    """
                    session.run(map_lyricist_query, song=song, lyricist=lyricist)
                    print(f"Song mapped to lyricist: {song} - {lyricist}")
                
                if bassist:
                    map_bassist_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (b:Artist {name: $bassist})
                    MERGE (s)-[:BASSIST]->(b)
                    """
                    session.run(map_bassist_query, song=song, bassist=bassist)
                    print(f"Song mapped to bassist: {song} - {bassist}")
                
                if dj:
                    map_dj_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (d:Artist {name: $dj})
                    MERGE (s)-[:DJ]->(d)
                    """
                    session.run(map_dj_query, song=song, dj=dj)
                    print(f"Song mapped to DJ: {song} - {dj}")
                
                if drummer:
                    map_drummer_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (d:Artist {name: $drummer})
                    MERGE (s)-[:DRUMMER]->(d)
                    """
                    session.run(map_drummer_query, song=song, drummer=drummer)
                    print(f"Song mapped to drummer: {song} - {drummer}")

        connector.close()

def main():
    # Neo4j connection details
    uri = "bolt://localhost:7687"
    username = "arun"
    password = "98765432"
    database = "music"

    # Specify the CSV file path
    csv_path = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/songs.csv"

    # Create nodes and relationships
    Neo4jConnector.create_nodes_and_relationships(csv_path, uri, username, password, database)

if __name__ == "__main__":
    main()
