from neo4j import GraphDatabase


class Neo4jClient:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute(self, query, parameters=None):
        with self.driver.session() as session:
            session.run(query, parameters or {})

    def execute_batch(self, query, records, batch_size=500):
        """
        Execute *query* using UNWIND over *records* in chunks of *batch_size*.

        The query must reference `$batch` and use `UNWIND $batch AS row`
        to iterate over the records.

        Example:
            query = '''
                UNWIND $batch AS row
                MERGE (c:Country {name: row.name})
            '''
            client.execute_batch(query, [{"name": "India"}, {"name": "Canada"}])
        """
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            with self.driver.session() as session:
                session.run(query, {"batch": chunk})

    def query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
