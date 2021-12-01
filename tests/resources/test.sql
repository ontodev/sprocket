CREATE TABLE test (
    'subject' TEXT,
    'weight' REAL,
    'weight_meta' TEXT
);
INSERT INTO test VALUES
("subject:1", 12.2, NULL),
("subject:2", NULL, 'json({"value": "NA", "nulltype": "empty"})'),
("subject:3", NULL, 'json({"value": "11g", "datatype": "trimmed_line", "messages": [{"rule": "datatype:number", "level": "error", "message": "Must be a number"}]})');
