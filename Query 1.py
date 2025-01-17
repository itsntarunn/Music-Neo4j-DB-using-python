from neo4j import GraphDatabase

class Neo4jDatabase:
    def __init__(self, uri, username, password, database):
        self._uri = uri
        self._username = username
        self._password = password
        self._database = database
        self._driver = GraphDatabase.driver(self._uri, auth=(self._username, self._password), database=self._database)

    def close(self):
        self._driver.close()

    def get_average_rating_for_user(self, username):
        with self._driver.session() as session:
            result = session.run(
                "MATCH (u:User {username: $username})-[r:RATED]-(:Song) "
                "WITH size(r.stars) AS numerical_rating "
                "RETURN AVG(numerical_rating) AS Average_Rating_User",
                username=username
            )
            return result.single()["Average_Rating_User"]

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "arun"
password = "98765432"
database_name = "music1"

# Example usage:
database = Neo4jDatabase(uri, username, password, database_name)
average_rating = database.get_average_rating_for_user("User 2")
database.close()

print("Average Rating given by User 2:", average_rating)
