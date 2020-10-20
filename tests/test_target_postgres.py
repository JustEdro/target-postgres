import target_postgres
import json
import types

schema_json = """{
        "type": "SCHEMA",
        "stream": "Applytobeapartner",
        "schema": {
            "selected": true,
            "name": "Apply to be a partner",
            "properties": {
                "id": {
                    "type": [
                        "null",
                        "string"
                    ],
                    "key": true
                },
                "ID (Submission token)": {
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "Test object": {
                    "type": "object",
                    "properties": {
                        "test id": {
                            "type": "string"
                        },
                        "test nullable": {
                            "type": [
                                "null",
                                "string"
                            ]
                        }
                    }
                }
            }
        },
        "key_properties": [
            "id"
        ]
    }"""


def test_flatten_schema():

    object = json.loads(schema_json)

    flattened_schema = target_postgres.db_sync.flatten_schema(d=object["schema"])

    expected_schema = {
         'id': {'key': True, 'type': ['null', 'string']},
         'id (submission token)': {'type': ['null', 'string']},
         'test object__test id': {'type': 'string'},
         'test object__test nullable': {'type': ['null', 'string']}
    }

    assert flattened_schema == expected_schema


def test_create_table_query_sanitized():

    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    create_table_statement = db_sync.create_table_query()

    assert create_table_statement == """CREATE TABLE test_schema.Applytobeapartner (id character varying, id__submission_token_ character varying, test_object__test_id character varying, test_object__test_nullable character varying, PRIMARY KEY (id))"""

def test_create_table_query_unsanitized():

    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": False
    }, object)

    create_table_statement = db_sync.create_table_query()

    assert create_table_statement == """CREATE TABLE test_schema.Applytobeapartner ("id" character varying, "id (submission token)" character varying, "test object__test id" character varying, "test object__test nullable" character varying, PRIMARY KEY ("id"))"""


def test_update_columns_sanitized():

    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    def get_table_columns(self, table_name):
        return [
            {
                "column_name": "id",
                "data_type": "character varying"
            },
            {
                "column_name": "to_remove",
                "data_type": "character varying"
            },
            {
                "column_name": "id__submission_token_",
                "data_type": "integer"
            },
        ]

    db_sync.get_table_columns = types.MethodType(get_table_columns, db_sync)

    queries = []

    def query(self, query, params=None):
        queries.append({ "query": query, "params": params})

    db_sync.query = types.MethodType(query, db_sync)

    db_sync.update_columns()

    assert len(queries) == 4
    assert queries == [
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN test_object__test_id character varying'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN test_object__test_nullable character varying'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner DROP COLUMN id__submission_token_'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN id__submission_token_ character varying'},
    ]

def test_update_columns_unsanitized():

    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": False
    }, object)

    def get_table_columns(self, table_name):
        return [
            {
                "column_name": "\"id\"",
                "data_type": "character varying"
            },
            {
                "column_name": "\"to remove\"",
                "data_type": "character varying"
            },
            {
                "column_name": "\"id (submission token)\"",
                "data_type": "integer"
            },
        ]

    db_sync.get_table_columns = types.MethodType(get_table_columns, db_sync)

    queries = []

    def query(self, query, params=None):
        queries.append({ "query": query, "params": params})

    db_sync.query = types.MethodType(query, db_sync)

    db_sync.update_columns()

    assert len(queries) == 4
    assert queries == [
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN "test object__test id" character varying'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN "test object__test nullable" character varying'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner DROP COLUMN "id (submission token)"'},
        {'params': None,
        'query': 'ALTER TABLE test_schema.Applytobeapartner ADD COLUMN "id (submission token)" character varying'},
    ]

def test_record_primary_key_string():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    record_json = """{
        "id": "rec0WLJrwkIBLwwBW",
        "ID (Submission token)": "zxagnl0m744hxi670a3mzxagnlonejlx"
    }"""

    primary_key_string = db_sync.record_primary_key_string(json.loads(record_json))

    assert primary_key_string == "rec0WLJrwkIBLwwBW"

def test_record_to_csv_line_sanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    record_json = """{
        "id": "rec0WLJrwkIBLwwBW",
        "ID (Submission token)": "zxagnl0m744hxi670a3mzxagnlonejlx",
        "Test object": {
            "test id": "testid",
            "test nullable": "testnullable"
        }
    }"""

    csv_line = db_sync.record_to_csv_line(json.loads(record_json))

    assert csv_line == '"rec0WLJrwkIBLwwBW","zxagnl0m744hxi670a3mzxagnlonejlx","testid","testnullable"'

def test_record_to_csv_line_unsanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": False
    }, object)

    record_json = """{
        "id": "rec0WLJrwkIBLwwBW",
        "ID (Submission token)": "zxagnl0m744hxi670a3mzxagnlonejlx",
        "Test object": {
            "test id": "testid",
            "test nullable": "testnullable"
        }
    }"""

    csv_line = db_sync.record_to_csv_line(json.loads(record_json))

    assert csv_line == '"rec0WLJrwkIBLwwBW","zxagnl0m744hxi670a3mzxagnlonejlx","testid","testnullable"'

def test_insert_from_temp_table_sanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    insert_line = db_sync.insert_from_temp_table()

    assert insert_line == 'INSERT INTO test_schema.Applytobeapartner (id, id__submission_token_, test_object__test_id, test_object__test_nullable)\n        (SELECT s.* FROM Applytobeapartner_temp s LEFT OUTER JOIN test_schema.Applytobeapartner t ON s.id = t.id WHERE t.id is null)\n        '
    
def test_update_from_temp_table_sanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": True
    }, object)

    insert_line = db_sync.update_from_temp_table()

    assert insert_line == 'UPDATE test_schema.Applytobeapartner SET id=s.id, id__submission_token_=s.id__submission_token_, test_object__test_id=s.test_object__test_id, test_object__test_nullable=s.test_object__test_nullable FROM Applytobeapartner_temp s\n        WHERE s.id = test_schema.Applytobeapartner.id\n        '

def test_insert_from_temp_table_unsanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": False
    }, object)

    insert_line = db_sync.insert_from_temp_table()

    assert insert_line == 'INSERT INTO test_schema.Applytobeapartner ("id", "id (submission token)", "test object__test id", "test object__test nullable")\n        (SELECT s.* FROM Applytobeapartner_temp s LEFT OUTER JOIN test_schema.Applytobeapartner t ON s."id" = t."id" WHERE t."id" is null)\n        '
    
def test_update_from_temp_table_unsanitized():
    object = json.loads(schema_json)

    db_sync = target_postgres.DbSync({
        "schema": "test_schema",
        "sanitize_column_names": False
    }, object)

    insert_line = db_sync.update_from_temp_table()

    assert insert_line == 'UPDATE test_schema.Applytobeapartner SET "id"=s."id", "id (submission token)"=s."id (submission token)", "test object__test id"=s."test object__test id", "test object__test nullable"=s."test object__test nullable" FROM Applytobeapartner_temp s\n        WHERE s."id" = test_schema.Applytobeapartner."id"\n        '
    