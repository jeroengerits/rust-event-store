CREATE TABLE batches (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream TEXT NOT NULL,
    event_ids TEXT NOT NULL CHECK (json_typeof(event_ids::json) = 'array'),
    contents TEXT NOT NULL CHECK (json_typeof(contents::json) = 'array'),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    first_position BIGINT NOT NULL CHECK (first_position >= 0),
    last_position BIGINT NOT NULL CHECK (last_position >= first_position)
);

CREATE TABLE events (
    position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    event_type TEXT NOT NULL,
    data TEXT NOT NULL CHECK (data::json IS NOT NULL),
    metadata TEXT CHECK (metadata IS NULL OR metadata::json IS NOT NULL),
    timestamp TEXT NOT NULL,
    batch_id BIGINT NOT NULL REFERENCES batches(id),
    UNIQUE (stream, revision)
);

CREATE FUNCTION prevent_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events are append-only';
END;
$$;
CREATE TRIGGER events_no_update_or_delete BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION prevent_event_mutation();
