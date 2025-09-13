from sqlalchemy import inspect, text

def safe_overwrite(engine, df, table, schema="public", keep_extra_cols=False):
    insp = inspect(engine)
    with engine.begin() as con:
        exists = insp.has_table(table, schema=schema)
        if not exists:
            df.to_sql(table, con, schema=schema, if_exists="replace", index=False)
            return
        cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
        if not keep_extra_cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
        con.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
        df.to_sql(table, con, schema=schema, if_exists="append", index=False)
