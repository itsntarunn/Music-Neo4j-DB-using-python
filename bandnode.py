from neo4j import GraphDatabase
import csv

class Neo4jHandler:
    def __init__(self, uri, username, password, database=None):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.database = database

    def close(self):
        self.driver.close()

    def create_band_node(self, name, formed, country):
        with self.driver.session(database=self.database) as session:
            query = (
                "MERGE (b:Band {name: $name, formed: $formed, country: $country})"
            )
            session.run(query, name=name, formed=formed, country=country)
            print(f"Band node created with name '{name}', formed '{formed}', and country '{country}'")

class BandProcessor:
    def __init__(self, handler, csv_file_path):
        self.handler = handler
        self.csv_file_path = csv_file_path

    def process_bands(self):
        with open(self.csv_file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            next(reader)  # Skip header row
            for row in reader:
                name, formed, country = row
                self.handler.create_band_node(name, formed, country)

def main():
    uri = "bolt://localhost:7687"
    username = "arun"
    password = "98765432"
    database = "music"  # Specify the database name
    bands_csv_path = "C:/Users/langi longi/Desktop/New folder/Neo4j/importcsvneo4j/bands.csv"

    handler = Neo4jHandler(uri, username, password, database)
    processor = BandProcessor(handler, bands_csv_path)
    processor.process_bands()
    handler.close()

if __name__ == "__main__":
    main()
