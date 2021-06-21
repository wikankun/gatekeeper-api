import re
import json
from dotenv import load_dotenv
from datetime import datetime
from google.cloud import bigquery

load_dotenv()


class Database:

    def __init__(self, dataset_id):
        self.tables = []
        self.dataset_id = dataset_id
        self.client = bigquery.Client()
        for table in self.client.list_tables(dataset_id):
            self.tables.append(table.table_id)

    def close(self):
        return self.client.close()

    def process(self, activity):
        # 1. insert operation
        if activity['operation'] == 'insert':
            self.insert(activity)
        # 2. delete operation
        elif activity['operation'] == 'delete':
            self.delete(activity)

    def insert(self, payload):
        # 1.a. table not available in db
        if payload['table'] not in self.tables:
            result = self.insert_create_table(payload)
            self.tables.append(payload['table'])
            self.insert_log(payload)
            return result

        # 1.b. table available in db
        else:
            # 1.b.1. field does not exist
            # 1.b.2. field exist
            result = self.insert_alter_table(payload)
            self.insert_log(payload)
            return result

    def delete(self, payload):
        # 2.a. table not available in db
        if payload['table'] not in self.tables:
            self.delete_nonexisting_table(payload)
        # 2.b. table available in db
        else:
            result = self.delete_existing_table(payload)
            self.insert_log(payload)
            return result

    def datatype_mapper(self, column_type):
        if column_type == 'TEXT' or 'VARCHAR' in column_type:
            return 'STRING'
        elif column_type == 'INTEGER':
            return 'INT64'
        elif 'FLOAT' in column_type or 'DOUBLE' in column_type:
            return 'FLOAT64'
        elif column_type == 'BOOLEAN':
            return 'BOOL'
        elif 'DECIMAL' in column_type:
            regex_result = re.findall(
                r'DECIMAL\((\d+),(\d+)\)', column_type)[0]
            return f'NUMERIC({regex_result[0]}{regex_result[1]})'
        return 'STRING'

    def name_type_pair(self, payload):
        paired_schema = f"ALTER TABLE {self.dataset_id}.{payload['table']} "
        column_names = payload['col_names']
        column_types = [self.datatype_mapper(
            column_type) for column_type in payload['col_types']]
        for index, (name, dtype) in enumerate(zip(column_names, column_types)):
            end_line = ", " if index != len(column_names)-1 else ""
            paired_schema += f'ADD COLUMN IF NOT EXISTS {name} {dtype}{end_line}'
        return paired_schema

    def value_name_pair(self, payload):
        paired_schema = ""
        column_names = payload['col_names']
        column_values = payload['col_values']
        for value, name in zip(column_values, column_names):
            if isinstance(value, int):
                paired_schema += f"{value} {name}, "
            else:
                paired_schema += f"'{value}' {name}, "
        return paired_schema

    def insert_create_table(self, payload):
        values = self.value_name_pair(payload)

        query = f"""CREATE TABLE {self.dataset_id}.{payload['table']} AS
            SELECT {values}"""
        return self.client.query(query).result()
    
    def insert_alter_table(self, payload):
        self.client.query(self.name_type_pair(payload)).result()
        columns = ",".join(payload['col_names'])
        values = str(tuple(payload['col_values']))

        query = f"""INSERT `{self.dataset_id}.{payload['table']}` ({columns})
            VALUES {values}"""
        return self.client.query(query).result()

    def delete_nonexisting_table(self, payload):
        error_msg = f"{payload['table']} not available"
        self.insert_error_log(payload, error_msg)
        raise ValueError(error_msg)

    def delete_existing_table(self, payload):
        where_stmnt = self.where_clause(
            payload['old_value']['col_names'],
            payload['old_value']['col_values'])

        query = f"""
            DELETE FROM {self.dataset_id}.{payload['table']}
            WHERE {where_stmnt}
        """
        return self.client.query(query).result()

    def where_clause(self, names, values):
        wheres = [f"{name} = {value}"
            if isinstance(value, int)
            else f"{name} = '{value}'"
            for name, value in zip(names, values)]
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
