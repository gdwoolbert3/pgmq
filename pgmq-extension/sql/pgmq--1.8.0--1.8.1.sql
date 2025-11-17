CREATE OR REPLACE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = (clock_timestamp() + $2)
        WHERE msg_id = ANY($1)
        RETURNING *;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg_ids, make_interval(secs => vt);
END;
$$ LANGUAGE plpgsql;
