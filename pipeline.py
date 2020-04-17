import logging
logging.basicConfig(level=logging.INFO)
import subprocess


logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']


def main():
    _extract()
    _transform()
    _load()


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        subprocess.run(['python', 'main.py', news_site_uid], cwd='./web_scrapper')
        subprocess.run(['find', '.', '-name', '{}*'.format(news_site_uid), 
                        '-exec', 'mv', '{}', '../transform_dataset/{}_.csv'.format(news_site_uid), ';'],
                       cwd='./web_scrapper')


def _transform():
    logger.info('Starting transform process')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = '{}_.csv'.format(news_site_uid)
        clean_data_filename = 'clean_{}'.format(dirty_data_filename)
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform_dataset')
        subprocess.run(['rm', dirty_data_filename], cwd='./transform_dataset')
        subprocess.run(['mv', clean_data_filename, '../load_into_db/{}.csv'.format(news_site_uid)], cwd='./transform_dataset')


def _load():
    logger.info('Starting loading process')
    for news_site_uid in news_sites_uids:
        clean_data_filename = '{}.csv'.format(news_site_uid)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load_into_db')
        subprocess.run(['rm', clean_data_filename], cwd='./load_into_db')


if __name__ == '__main__':
    main()