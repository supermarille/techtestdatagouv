"""
20/11/2020 - Marie Cheng - Tech test

"""
import os
import psycopg2
import re
import requests
import shutil


DATASETS_DIRECTORY = '../datasets'


class Scraping:

    dataset_directory = DATASETS_DIRECTORY
    db_conn = None
    cursor = None

    def __init__(self):
        # connect and create database
        # self.db_conn = psycopg2.connect(database="docker", user='docker', password='docker',
        #                                 host='postgres', port='5432')
        self.db_conn = psycopg2.connect(database='datasetsdb', user='postgres', password='',
                                        host='localhost')
        self.db_conn.autocommit = True
        self.cur = self.db_conn.cursor()
        # create a table for the links to save
        self.cur.execute("""DROP TABLE IF EXISTS links;
                           CREATE TABLE links(latest_url VARCHAR(255),
                                              title VARCHAR(255),
                                              filepath VARCHAR(255));
                        """)

    def close_db(self):
        self.db_conn.close()

    def get_json_data(self, url):
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

    def parse_useful_data(self, response):
        """
        Get data from response
        :param response: response as json
        :return: a dict with next_page_url and title_latest dict
        """
        data = response['data']
        next_page_url = response['next_page']
        title_latest = dict()
        for item in data:
            for resource in item['resources']:
                title_latest[resource['latest']] = resource['title']
        return {'next_page_url': next_page_url,
                'title_latest': title_latest}

    def store(self, data):
        """Store data in database"""
        for latest_url, title in data.items():
            self.cur.execute(f"""INSERT INTO links(latest_url, title)
                                VALUES ('{latest_url}', '{title.replace("'", "''")}');
                         """)

    def download_dataset(self, latest_url):
        """
        Download dataset from `latest_url` and store them in a local folder `directory_path`.
        :param latest_url: url to download file from
        """
        print(f"Downloading file from {latest_url}, might take some time ---")
        # create folder if not exists
        if not os.path.exists(self.dataset_directory):
            os.makedirs(self.dataset_directory)
        try:
            res = requests.get(latest_url, allow_redirects=True)
            res.raise_for_status()
        except requests.exceptions.RequestException as err:
            print(err)
            self.cur.execute(f"""DELETE FROM links
                                 WHERE latest_url = '{latest_url}';
                              """)
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
        filepath = os.path.join(self.dataset_directory, filename)
        open(filepath, 'wb').write(res.content)
        print(f"File saved in {filepath}")
        # save file path in db
        self.cur.execute(f"""UPDATE links
                             SET filepath = '{filepath}'
                             WHERE latest_url = '{latest_url}';
                          """)

    def download_datasets_from_page(self, url_dict):
        """Save the datasets from dict of urls to directory path
        :param url_dict: dict which keys are titles and values are url to download file from
        """
        for latest_url in url_dict.keys():
            self.download_dataset(latest_url)

    def scrap_pages(self, url, nb_pages=5):
        """
        Parse and save data starting from the given url up to the following `nb_pages` pages.
        :param url: start scraping from this url
        :param nb_pages: number of pages to scrap after the given url ; defaults to 5
        :return: True if no errors were encountered
        """
        if nb_pages >= 0:
            response, error = self.get_json_data(url)
            if error is not None:
                print(error)
                return False
            data = self.parse_useful_data(response)
            self.store(data['title_latest'])
            self.download_datasets_from_page(data['title_latest'])
            self.scrap_pages(data['next_page_url'], nb_pages - 1)
        else:
            return True


def main():
    if os.path.exists(DATASETS_DIRECTORY):
        shutil.rmtree(DATASETS_DIRECTORY)
    # url = "https://www.data.gouv.fr/api/1/datasets/?page=3000&page_size=1"
    url = "https://www.data.gouv.fr/api/1/datasets/?page=3001&page_size=1"
    s = Scraping()
    s.scrap_pages(url)

    s.close_db()


if __name__ == "__main__":
    main()
