from flask import Flask, send_file, render_template
import psycopg2
import os
#from setup_db.scraping import DATASETS_DIRECTORY


app = Flask(__name__)

DATASETS_DIRECTORY = "../datasets"


def connect_to_db():
    # conn = psycopg2.connect(database="docker", user='docker', password='docker',
    #                         host='postgres', port='5432')
    conn = psycopg2.connect(database="postgres", user="postgres", password="test", host="localhost")
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
        if path is None:
            continue
        print(path)
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
    # app.run(host='0.0.0.0')
    app.run()

