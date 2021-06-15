from google.cloud import bigquery


class Database:

    def __init__(self, dataset_id):
        self.tables = []
        self.client = bigquery.Client()
        for table in self.client.list_tables(dataset_id):
            self.tables.append(table.table_id)

    def insert(self, payload):
        # 1.a. table not available in db
        if payload['table'] not in self.tables:
            values = ""
            for i in range(len(payload['col_names'])):
                if payload['col_types'][i] == 'INTEGER':
                    values += f"{payload['col_values'][i]} {payload['col_names'][i]}\n"
                else:
                    values += f"'{payload['col_values'][i]}' {payload['col_names'][i]}\n"

            query = f"CREATE TABLE {payload['table']} AS\n" \
                "SELECT " + values

            print(query)

        # 1.b. table available in db
        else:
            pass

            # 1.b.1. field does not exist

            # 1.b.2. field exist

        pass

    def delete(self, payload):
        # 2.a. table not available in db
        if payload['table'] not in self.tables:
            print("Error, table not available")
        # 2.b. table available in db
        else:
            pass
            # 2.b.1. field exist

            # 2.b.2. field does not exist

        pass

    def insert_log(self):
        pass

    def insert_error_log(self):
        pass
