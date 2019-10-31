
def schema_diff(parent_tablename, child_tablename, conn):
    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"
    # conn.cursor.execute(sql % parent_tablename)
    # parent_schema = conn.cursor.fetchall()
    # conn.cursor.execute(sql % child_tablename)
    # child_schema = conn.cursor.fetchall()

    cursor = conn.cursor()
    cursor.execute(sql % parent_tablename)
    parent_schema = cursor.fetchall()
    cursor.execute(sql % child_tablename)
    child_schema = cursor.fetchall()

    return schema_diff_helper(parent_schema, child_schema)

def schema_diff_helper(parent_schema, child_schema):
    parent_schema, child_schema = set(parent_schema), set(child_schema)

    deletions = parent_schema - child_schema
    additions = child_schema - parent_schema

    deletions_t = [tuple(attname for attname, atttype in deletions), tuple(atttype for attname, atttype in deletions)]
    additions_t = [tuple(attname for attname, atttype in additions), tuple(atttype for attname, atttype in additions)]

    edits = set(deletions_t[0]).intersection(set(additions_t[0]))
    if edits:
        edits = [(a, deletions_t[1][deletions_t[0].index(a)], additions_t[1][additions_t[0].index(a)]) for a in edits]

    return list(deletions), list(additions), list(edits)