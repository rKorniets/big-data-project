import os
import requests
import tarfile

def download_and_extract(url, target_path):
    def extract():
        print('Extracting {}...'.format(target_path))
        # for tar files
        tar = tarfile.open(target_path, "r:")
        tar.extractall('.\data')
        tar.close()

    #check if file already exists
    if os.path.exists(target_path):
        print('{} already exists, skipping download'.format(target_path))
        return

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        print('Downloading and extracting {}...'.format(url))
        with open(target_path, 'wb') as f:
            f.write(response.raw.read())
        print('Done!\n')
        extract()
    else:
        raise 'HTTP response code is not 200'

#check if datafolder exists
if not os.path.exists('data'):
    os.makedirs('data')

name_url = 'https://datasets.imdbws.com/name.basics.tsv.gz'
target_path1 = 'data/name.basics.tsv.gz'
download_and_extract(name_url, target_path1)

title_url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
target_path2 = 'data/title.basics.tsv.gz'
download_and_extract(title_url, target_path2)

crew_url = 'https://datasets.imdbws.com/title.crew.tsv.gz'
target_path3 = 'data/title.crew.tsv.gz'
download_and_extract(crew_url, target_path3)

episode_url = 'https://datasets.imdbws.com/title.episode.tsv.gz'
target_path4 = 'data/title.episode.tsv.gz'
download_and_extract(episode_url, target_path4)

principals_url = 'https://datasets.imdbws.com/title.principals.tsv.gz'
target_path5 = 'data/title.principals.tsv.gz'
download_and_extract(principals_url, target_path5)

ratings_url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
target_path6 = 'data/title.ratings.tsv.gz'
download_and_extract(ratings_url, target_path6)

akas_url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
target_path7 = 'data/title.akas.tsv.gz'
download_and_extract(akas_url, target_path7)



