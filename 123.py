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
            # Add unique constraints for nodes
            unique_constraints = [
                "CREATE CONSTRAINT FOR (s:Song) REQUIRE s.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (p:Playlist) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (a:Album) REQUIRE a.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (b:Band) REQUIRE b.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (l:Artist) REQUIRE l.name IS UNIQUE",
                "CREATE CONSTRAINT FOR (u:User) REQUIRE u.username IS UNIQUE"
                # Add unique constraints for other node labels as needed
            ]
            for constraint in unique_constraints:
                session.run(constraint)

            # Add nodes and relationships
            for index, row in df.iterrows():
                song = row['Song']
                playlist = row['Playlist']
                album = row['Album']
                lyricist = row.get('Lyricist')
                bassist = row.get('Bassist')
                dj = row.get('DJ')
                drummer = row.get('Drummer')
                band = row.get('Band')  # Assuming 'Band' is the column containing band names
                ratings = [row[f'User {i} Rating'] for i in range(1, 4)]  # Get ratings for User 1, User 2, User 3

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
                if lyricist and lyricist != 'Unknown':
                    map_lyricist_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (l:Artist {name: $lyricist})
                    MERGE (s)-[:LYRICIST]->(l)
                    """
                    session.run(map_lyricist_query, song=song, lyricist=lyricist)
                    print(f"Song mapped to lyricist: {song} - {lyricist}")

                if bassist and bassist != 'Unknown':
                    map_bassist_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (b:Artist {name: $bassist})
                    MERGE (s)-[:BASSIST]->(b)
                    """
                    session.run(map_bassist_query, song=song, bassist=bassist)
                    print(f"Song mapped to bassist: {song} - {bassist}")

                if dj and dj != 'Unknown':
                    map_dj_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (d:Artist {name: $dj})
                    MERGE (s)-[:DJ]->(d)
                    """
                    session.run(map_dj_query, song=song, dj=dj)
                    print(f"Song mapped to DJ: {song} - {dj}")

                if drummer and drummer != 'Unknown':
                    map_drummer_query = """
                    MATCH (s:Song {name: $song})
                    MERGE (d:Artist {name: $drummer})
                    MERGE (s)-[:DRUMMER]->(d)
                    """
                    session.run(map_drummer_query, song=song, drummer=drummer)
                    print(f"Song mapped to drummer: {song} - {drummer}")

                # Map ratings to song using numerical values
                for i, rating in enumerate(ratings, start=1):
                    if rating:
                        map_rating_query = f"""
                        MATCH (s:Song {{name: $song}})
                        MERGE (u:User {{username: $username}})
                        MERGE (s)-[:RATED {{rating: $rating}}]->(u)
                        """
                                               session.run(map_rating_query, song=song, username=f"User {i}", rating=rating)
                        print(f"Rating {rating} mapped to Song {song} and User {i}")

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

