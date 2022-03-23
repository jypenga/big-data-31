def build_query(conn, subset, first_table='endYear'):
    names = conn.execute('PRAGMA show_tables').fetchdf().name

    specs, feats = [], []
    query = f'SELECT {subset}_{first_table}.tconst,'
    query += '\n'

    # feature loop
    for name in names:
        if name.startswith(subset):
            spec, feat = name.split('_')
            query += f'{spec}_{feat}.{feat}, '

            specs.append(spec)
            feats.append(feat)
            # query += '\n'

    query = query[:-2] + ' '
    query += f'FROM {subset}_{first_table} '

    # join loop
    for spec, feat in zip(specs, feats):
        if feat.endswith(first_table):
            continue
        elif feat.endswith('Ratings'):
            query += f'FULL OUTER JOIN {spec}_{feat} ON {spec}_{feat}.tconst = {subset}_{first_table}.tconst '
        else:
            query += f'INNER JOIN {spec}_{feat} ON {spec}_{feat}.tconst = {subset}_{first_table}.tconst '
        # query += '\n'

    # print(query)
    return query