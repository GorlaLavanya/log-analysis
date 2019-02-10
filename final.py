#!/usr/bin/env python3
import psycopg2
import datetime

query1 = ("SELECT title, count(*) FROM articles JOIN log "
          "ON log.path = CONCAT('/article/', articles.slug) "
          "GROUP BY title, path ORDER BY count(*) DESC limit 3;")

query2 = ("SELECT name, count(*) FROM authors JOIN articles ON "
          "articles.author = authors.id JOIN log ON log.path "
          "LIKE concat('%', articles.slug, '%') GROUP BY name,"
          "log.path ORDER BY count(*) DESC limit 3;")

query3 = ("""SELECT day, errors*100/total as percentage
        FROM (SELECT time::date AS day,
        COUNT(status) AS total,
        SUM((status NOT LIKE '%200%')::int)::float AS errors
        FROM log
        GROUP BY day) AS pretable
        WHERE errors/total > 0.01""")


def top_three_articles():
    print("what are the most popular three articles of all time")
    con.execute(query1)
    rows = con.fetchall()
    count = 0
    for row in rows:
        count = count+1
        print("\t" + str(count) + "." + str(row[0]) +
              "-" + str(row[1]) + "views")


def top_three_authors():
    print("what are the most popular authors of all time")
    con.execute(query2)
    rows = con.fetchall()
    count = 0
    for row in rows:
        count = count+1
        print ("\t" + str(count) + "." + str(row[0]) +
               "-" + str(row[1]) + "views")


def top_error_days():
    print("on which day did more than 1% of request lead to error")
    con.execute(query3)
    rows = con.fetchall()
    count = 0
    for (day, percent) in rows:
        print("    " + str(day) + ' - ' + str(round(percent, 2)) + '% errors')


def get_query(query):
    return rows


if __name__ == '__main__':
    db = psycopg2.connect(database='news')
    con = db.cursor()
    top_three_articles()
    top_three_authors()
    top_error_days()
    db.close()
