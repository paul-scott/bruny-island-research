import argparse
import logging
import datetime as dt
import psycopg2 as sql
import json


def resource_to_bigint(resource):
    """ Convert resource_id type to postgres bigint.

    bigint is signed, so need subtract by offset.
    Another option would be to pad it as UUID.
    """
    return int(resource.replace('-', ''), 16) - (16**16 // 2)


def select_estimated_actuals(conn, rid, fr, to):
    with conn:
        with conn.cursor() as curs:
            curs = conn.cursor()
            curs.execute("SELECT time, data->'ghi'"
                         + " FROM estimated_actuals AS t"
                         + " WHERE t.resource = %s"
                         + " AND time BETWEEN %s AND %s"
                         + " ORDER BY time ASC",
                         (rid, fr, to))
            return list(curs.fetchall())


def last_5_min(time):
    time -= dt.timedelta(seconds=time.second, microseconds=time.microsecond)
    time -= dt.timedelta(minutes=(time.minute % 5))

    return time


def parse_time(time_str):
    """ Parse the returned time format

    Doesn't conform to iso8601 standard.
    The "microseconds" have 7 digits so doesn't work with %f.
    Don't care about sub-seconds.
    This will break if they ever change this.
    """
    time, rhs = time_str.split('.')
    if rhs[-1] != 'Z':
        raise ValueError('Time not in UTC: {}'.format(time_str))
    return dt.datetime.strptime(time + '+0000', '%Y-%m-%dT%H:%M:%S%z')


def extract_forecasts_data(row):
    t_for = last_5_min(row[0])

    r = []
    r.append(t_for)
    for i, data in enumerate(row[1]):
        t_expect = t_for + (i + 1) * dt.timedelta(minutes=5)
        t = parse_time(data['period_end'])
        assert(t_expect == t)
        ghi = data['ghi']
        r.append(ghi)
    return r


def select_forecasts(conn, rid, fr, to):
    with conn:
        with conn.cursor() as curs:
            curs = conn.cursor()
            curs.execute("SELECT time, data"
                         + " FROM forecasts AS t"
                         + " WHERE t.resource = %s"
                         + " AND time BETWEEN %s AND %s"
                         + " ORDER BY time ASC",
                         (rid, fr, to))
            return [extract_forecasts_data(row) for row in curs.fetchall()]


def process(conn, rid, fr, to):
    rid_int = resource_to_bigint(rid)
    actuals = select_estimated_actuals(conn, rid_int, fr, to)
    logging.info('Actuals contains {} rows'.format(len(actuals)))
    actuals_fn = 'est_actuals_{}_{}_{}.csv'.format(
        rid,
        fr.strftime('%Y-%m-%dT%H:%M:%S%z'),
        to.strftime('%Y-%m-%dT%H:%M:%S%z'),
    )
    with open(actuals_fn, 'w') as f:
        for row in actuals:
            r = row[0].strftime('%Y-%m-%dT%H:%M:%S%z')
            r += ',' + str(row[1])
            r += '\n'
            f.write(r)

    forecasts_fn = 'forecasts_{}_{}_{}.csv'.format(
        rid,
        fr.strftime('%Y-%m-%dT%H:%M:%S%z'),
        to.strftime('%Y-%m-%dT%H:%M:%S%z'),
    )
    with open(forecasts_fn, 'w') as f:
        # Reading forecasts is slow, cutting into chunks
        fr_chunk = fr
        to_chunk = fr_chunk + dt.timedelta(days=2)
        while True:
            if fr_chunk >= to:
                break
            logging.info('Chunk {}'.format(fr_chunk))
            to_chunk = min(to, to_chunk)
            forecasts = select_forecasts(conn, rid_int, fr_chunk, to_chunk)
            logging.info('Forecasts contains {} rows'.format(len(forecasts)))
            for row in forecasts:
                r = row[0].strftime('%Y-%m-%dT%H:%M:%S%z')
                r += ','
                r += ','.join([str(v) for v in row[1:]])
                r += '\n'
                f.write(r)
            fr_chunk = to_chunk
            to_chunk = fr_chunk + dt.timedelta(days=2)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='database name')
    parser.add_argument('rid',
                        help='resource id file or value')
    parser.add_argument('--fr',
                        help='iso8601 datetime from (default: now - 24hrs)')
    parser.add_argument('--to',
                        help='iso8601 datetime to (default: now)')
    args = parser.parse_args()

    to = dt.datetime.now(tz=dt.timezone.utc)
    fr = to - dt.timedelta(hours=24)

    if args.to is not None:
        to = dt.datetime.strptime(args.to, '%Y-%m-%dT%H:%M:%S%z')
        if args.fr is None:
            fr = to - dt.timedelta(hours=24)

    if args.fr is not None:
        fr = dt.datetime.strptime(args.fr, '%Y-%m-%dT%H:%M:%S%z')
        if args.to is None:
            to = fr + dt.timedelta(hours=24)

    #conn = sql.connect(dbname=args.database)
    conn = sql.connect(dbname=args.database, host='localhost', port=5433)

    rids = []
    try:
        rfile = json.load(open(args.rid))
        for r in rfile['sites']:
            rids.append(r['resource_id'])

    except:
        rids = [args.rid]

    print(rids)
    for rid in rids:
        process(conn, rid, fr, to)
