#!/usr/bin/env python3

import psycopg2

DATABASE_NAME = "news"

query_top_articles = '''
select a.title, count(*) as views
    from articles as a, log as l
    where l.path like ('%' || a.slug)
    group by a.title
    order by views desc
    limit 3;
'''

query_top_authors = '''
select au.name, count(*) as views
    from articles as a, log as l, authors as au
    where l.path like ('%' || a.slug)
    and au.id = a.author
    group by au.name
    order by views desc;
'''

query_top_errors = '''
select result.req_time as "Date",
to_char(result.err_percent, '999D99') as "% of Errors"
    from (
        select date(time) as REQ_TIME,
        100*(count(*) filter(where status like ('4%')
        or status like ('5%')))/count(*)::float as ERR_PERCENT
        from log
        group by REQ_TIME
    ) as result
where result.err_percent > 1.0;
'''


def select_query(query):
    """ Executes select query only
    returns results of select query"""
    db = psycopg2.connect(database=DATABASE_NAME)
    cur = db.cursor()
    cur.execute(query)
    entries = cur.fetchall()
    db.close()
    return entries


def print_top_errors():
    """Prnts Days with > 1% error reqests"""
    entries = select_query(query_top_errors)
    # Print table header
    print("\nMore then 1% Errors were on")
    print("------------------------------------+-----------")
    print('{: ^35s} | {: ^8}'.format("Date", "Errors, %"))
    print("------------------------------------+-----------")

    for entry in entries:
        print(" %-34s | %s" % (entry[0], entry[1]))


def print_top_articles():
    """Prints top 3 most viewed articles"""
    entries = select_query(query_top_articles)
    # Print table header
    print("\nTop 3 Articles")
    print("------------------------------------+-----------")
    print('{: ^35s} | {: ^8}'.format("Title", "Views"))
    print("------------------------------------+-----------")

    for entry in entries:
        print(" %-34s | %d" % (entry[0], entry[1]))


def print_top_auth():
    """Prints sorted list of authors most to least popular"""
    entries = select_query(query_top_authors)
    # Print table header
    print("\nThe Top Most Viewed Authors of All Times")
    print("------------------------------------+-----------")
    print('{: ^35s} | {: ^8}'.format("Authors", "Views"))
    print("------------------------------------+-----------")

    for entry in entries:
        print(" %-34s | %d" % (entry[0], entry[1]))


if __name__ == '__main__':
    print_top_articles()
    print_top_auth()
    print_top_errors()
