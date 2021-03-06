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


def print_header(header, col_title_1, col_title_2):
    """Prints table header"""
    print("\n" + header)
    print("------------------------------------+-----------")
    print('{: ^35s} | {: ^8}'.format(col_title_1, col_title_2))
    print("------------------------------------+-----------")


def print_top_errors():
    """Prnts Days with > 1% error reqests"""
    entries = select_query(query_top_errors)
    # Print table header
    header = "More then 1% of errors occured on"
    col_title_1 = "Date"
    col_title_2 = "Errors, %"
    print_header(header, col_title_1, col_title_2)

    for entry in entries:
        print(" %-34s | %s" % (entry[0], entry[1]))


def print_top_articles():
    """Prints top 3 most viewed articles"""
    entries = select_query(query_top_articles)
    # Print table header
    header = "Top 3 Articles"
    col_title_1 = "Title"
    col_title_2 = "Views"
    print_header(header, col_title_1, col_title_2)

    for entry in entries:
        print(" %-34s | %d" % (entry[0], entry[1]))


def print_top_auth():
    """Prints sorted list of authors most to least popular"""
    entries = select_query(query_top_authors)
    # Print table header
    header = "The Top Most Viewed Authors of All Times"
    col_title_1 = "Authors"
    col_title_2 = "Views"
    print_header(header, col_title_1, col_title_2)

    for entry in entries:
        print(" %-34s | %d" % (entry[0], entry[1]))


if __name__ == '__main__':
    print_top_articles()
    print_top_auth()
    print_top_errors()
