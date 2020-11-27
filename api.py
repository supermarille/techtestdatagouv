from flask import Flask, send_file, render_template
import psycopg2
import os
from scraping import DATASETS_DIRECTORY


app = Flask(__name__)


def connect_to_db():
    conn = psycopg2.connect(database="datasetsdb", user='postgres', password='',
                            host='127.0.0.1', port='5432')
    conn.autocommit = True
    return conn


@app.route('/filesmetadatalist')
def list_files_metadata():
    """Get the list of tuples (title, latest_url) from db."""
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("""SELECT *
                   FROM links;
                """)
    results = dict()
    for url, title, path in cur.fetchall():
        results[url] = {'title': title,
                        'filename': path.split('/')[-1]}
    conn.close()
    return render_template('page.html', data=results)


@app.route('/download/<filename>')
def download_file(filename):
    file = open(os.path.join(DATASETS_DIRECTORY, filename), 'r')
    try:
        return send_file(file)
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    app.run()
