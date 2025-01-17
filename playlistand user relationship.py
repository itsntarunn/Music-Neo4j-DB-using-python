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
    def create_relationships(cls, playlist_csv_path, uri, username, password, database):
        connector = cls(uri, username, password, database)
        connector.connect()

        try:
            playlist_df = pd.read_csv(playlist_csv_path, delimiter=';')
            print("Playlist CSV file loaded successfully")
        except Exception as e:
            print(f"Failed to load Playlist CSV file: {e}")
            return

        with connector.driver.session() as session:
            for index, row in playlist_df.iterrows():
                playlist_name = row['playlist name']
                user_id = row['fav user']

                # Create relationship between user and playlist
                create_relationship_query = """
                MATCH (u:User {user_id: $user_id})
                MATCH (p:Playlist {name: $playlist_name})
                MERGE (u)-[:FAVOURITE]->(p)
                """
                session.run(create_relationship_query, user_id=user_id, playlist_name=playlist_name)
                print(f"Relationship created between User '{user_id}' and Playlist '{playlist_name}'")

        connector.close()

def main():
    # Neo4j connection details
    uri = "bolt://localhost:7687"
    username = "arun"  # Replace with your Neo4j username
    password = "98765432"  # Replace with your Neo4j password
    database = "music"

    # Specify the CSV file path for playlists
    playlist_csv_path = r"C:\Users\langi longi\Desktop\New folder\Neo4j\importcsvneo4j\playlist.csv"

    # Create relationships between users and playlists
    Neo4jConnector.create_relationships(playlist_csv_path, uri, username, password, database)

if __name__ == "__main__":
    main()
