"""
20/11/2020 - Marie Cheng - Tech test

"""
import os
import psycopg2
import re
import requests
import shutil
import pprint


DATASETS_DIRECTORY = '../datasets'


def get_json_data(url):
    """
    Get the data from the given url
    :param url: url to get data from
    :return: data from the page as dict or raise HTTPError
    """
    try:
        res = requests.get(url)
        res.raise_for_status()
    except requests.HTTPError as http_err:
        return None, http_err
    return res.json(), None


def parse_useful_data(response):
    """
    Get data from response
    :param response: response as json
    :return: a dict with page_id, next_page_url and title_latest dict
    """
    data = response['data']
    next_page_url = response['next_page']
    page_id = response['page']
    title_latest = dict()
    for item in data:
        for resource in item['resources']:
            title_latest[resource['id']] = {'title': resource['title'],
                                            'latest_url': resource['latest']}
    return {'page_id': page_id,
            'next_page_url': next_page_url,
            'title_latest': title_latest}


def store(data, db_conn):
    """store data in database"""
    cur = db_conn.cursor()
    for link_id, values in data.items():
        cur.execute(f"""INSERT INTO links(id, title, latest_url)
                            VALUES ('{link_id}', '{values['title']}', '{values['latest_url']}');
                     """)


def download_dataset(latest_url, directory_path):
    """
    Download dataset from `latest_url` and store them in a local folder `directory_path`.
    :param latest_url: url to download file from
    :param directory_path: path to the directory to save the file
    """
    print(f"Downloading file from {latest_url}, might take some time ---")
    # create folder if not exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    try:
        res = requests.get(latest_url, allow_redirects=True)
        res.raise_for_status()
    except requests.HTTPError as http_err:
        print(http_err)
        return
    # get the dataset file name if it exists, else just take the last part of the url
    cd = res.headers.get('content-disposition')
    if not cd:
        filename = latest_url.rsplit('/', 1)[1]
    else:
        filename = re.findall('filename=(.+)', cd)
        if len(filename) == 0:
            filename = latest_url.rsplit('/', 1)[1]
        filename = filename[0].replace('"', '')
    filepath = os.path.join(directory_path, filename)
    open(filepath, 'wb').write(res.content)
    print(f"File saved in {filepath}")


def download_datasets_from_page(url_dict, directory_path):
    """Save the datasets from dict of urls to directory path
    :param url_dict: dict which keys are titles and values are url to download file from
    :param directory_path: path to the directory to save the files
    """
    for id, values in url_dict.items():
        download_dataset(values['latest_url'], directory_path)


def scrap_pages(url, db_conn, nb_pages=5):
    """
    Parse and save data starting from the given url up to the following `nb_pages` pages.
    :param url: start scraping from this url
    :param db_conn: database in use
    :param nb_pages: number of pages to scrap after the given url ; defaults to 5
    :return: True if no errors were encountered
    """
    if nb_pages >= 0:
        response, error = get_json_data(url)
        if error is not None:
            print(error)
            return False
        data = parse_useful_data(response)
        store(data['title_latest'], db_conn)
        download_datasets_from_page(data['title_latest'], DATASETS_DIRECTORY)
        scrap_pages(data['next_page_url'], db_conn, nb_pages - 1)
    else:
        return True


def main():
    """ """
    if os.path.exists(DATASETS_DIRECTORY):
        shutil.rmtree(DATASETS_DIRECTORY)
    # url = "https://www.data.gouv.fr/api/1/datasets/?page=3000&page_size=1"
    url = "https://www.data.gouv.fr/api/1/datasets/?page=3001&page_size=1"
    # connect and create database
    conn = psycopg2.connect(database="datasetsdb", user='postgres', password='',
                            host='127.0.0.1', port='5432')
    conn.autocommit = True
    cur = conn.cursor()
    # create a table for the links to save
    cur.execute("""DROP TABLE IF EXISTS links;
                   CREATE TABLE links(id VARCHAR(255),
                                      title VARCHAR(255),
                                      latest_url VARCHAR(255));
                """)
    scrap_pages(url, conn)

    conn.close()


if __name__ == "__main__":
    main()
