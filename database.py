import json
from datetime import datetime
from google.cloud import bigquery


class Database:

    def __init__(self, dataset_id):
        self.tables = []
        self.dataset_id = dataset_id
        self.client = bigquery.Client()
        for table in self.client.list_tables(dataset_id):
            self.tables.append(table.table_id)

    def close(self):
        return self.client.close()

    def insert(self, payload):
        # 1.a. table not available in db
        values = ""
        if payload['table'] not in self.tables:
            for i in range(len(payload['col_names'])):
                if payload['col_types'][i] == 'INTEGER':
                    values += f"{payload['col_values'][i]} {payload['col_names'][i]},\n"
                else:
                    values += f"'{payload['col_values'][i]}' {payload['col_names'][i]},\n"

            query = f"CREATE TABLE {self.dataset_id}.{payload['table']} AS\n" \
                "SELECT " + values
            result = self.client.query(query).result()
            self.tables.append(payload['table'])
            # log success
            self.insert_log(payload)
            return result

        # 1.b. table available in db
        else:

            # 1.b.1. field does not exist
            columns = ",".join(payload['col_names'])
            for i in range(len(payload['col_names'])):
                if payload['col_types'][i] == 'INTEGER':
                    values += f"{payload['col_values'][i]},"
                else:
                    values += f"'{payload['col_values'][i]}',"

            query = f"INSERT {self.dataset_id}.{payload['table']}\n" \
                f"({columns}) VALUES \n" + "(" + values[:-1] + ")"
            result = self.client.query(query).result()
            # log success
            self.insert_log(payload)
            return result

            # 1.b.2. field exist

    def delete(self, payload):
        # 2.a. table not available in db
        if payload['table'] not in self.tables:
            error_msg = f"{payload['table']} not available"
            # log error / raise
            self.insert_error_log(payload, error_msg)
            return error_msg
        # 2.b. table available in db
        else:
            # 2.b.1. field exist
            where_stmnt = self.where_statement(
                payload['old_value']['col_names'],
                payload['old_value']['col_values'])
            query = f"""
                DELETE FROM {self.dataset_id}.{payload['table']}
                WHERE {where_stmnt}
            """

            result = self.client.query(query).result()
            # log success
            self.insert_log(payload)
            return result

            # 2.b.2. field does not exist


    def where_statement(self, columns, values):
        wheres = []
        for i in range(len(columns)):
            if isinstance(values[i], str):
                wheres.append(f"{columns[i]} = '{values[i]}'")
            else:
                wheres.append(f"{columns[i]} = {values[i]}")
        return " AND ".join(wheres)

    def insert_log(self, payload):
        query = f"""
            INSERT {self.dataset_id}.activity_log
            (payload, created_at, is_error)
            VALUES
            ('{json.dumps(payload)}', '{datetime.now()}', false)
        """

        return self.client.query(query).result()

    def insert_error_log(self, payload, error_message):
        query = f"""
            INSERT {self.dataset_id}.activity_log
            (payload, created_at, is_error, error_message)
            VALUES
            ('{json.dumps(payload)}', '{datetime.now()}', true, '{error_message}')
        """

        return self.client.query(query).result()
