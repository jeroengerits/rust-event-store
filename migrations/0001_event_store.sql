CREATE TABLE batches (
    id INTEGER PRIMARY KEY,
    stream TEXT NOT NULL,
    event_ids TEXT NOT NULL CHECK (json_valid(event_ids) AND json_type(event_ids) = 'array'),
    contents TEXT NOT NULL CHECK (json_valid(contents) AND json_type(contents) = 'array'),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    first_position INTEGER NOT NULL CHECK (first_position >= 0),
    last_position INTEGER NOT NULL CHECK (last_position >= first_position)
);

CREATE TABLE events (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    event_type TEXT NOT NULL,
    data TEXT NOT NULL CHECK (json_valid(data)),
    metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
    timestamp TEXT NOT NULL,
    batch_id INTEGER NOT NULL REFERENCES batches(id),
    UNIQUE (stream, revision)
);

CREATE TRIGGER events_no_update BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;

CREATE TRIGGER events_no_delete BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
