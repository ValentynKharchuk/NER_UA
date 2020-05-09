from pymongo import MongoClient
import requests
import tempfile
import os
import logging
logger = logging.getLogger(__name__)

MONGO_URL = ('VESUM_MONGO_DB_URL' in os.environ and os.environ['VESUM_MONGO_DB_URL']) or 'localhost:27017'
DB_NAME = ('VESUM_DB_NAME' in os.environ and os.environ['VESUM_DB_NAME']) or 'natasha-uk-database'
DEFAULT_DICTIONARY_URL = ('DEFAULT_DICTIONARY_URL' in os.environ and os.environ['DEFAULT_DICTIONARY_URL']) or 'https://dl.dropboxusercontent.com/s/6egyjiexh12z23b/dict_corp_lt.txt?dl\=0'
BUFFER_LIMIT = 10000
client = MongoClient(MONGO_URL, maxPoolSize=20)


class VesumService:
    def find_by_word_form(self, word_form):
        return client[DB_NAME]['vesum-entry'].find({'word': word_form})

    def init_vesum(self):
        if self.find_by_word_form('ящурні').count() > 0:
            return
        self.refresh_dictionary()

    def refresh_dictionary(self, dictionary_url=DEFAULT_DICTIONARY_URL):
        logger.warning('VESUM refresh start')
        client[DB_NAME]['vesum-entry'].drop()
        dictionary_url = dictionary_url or DEFAULT_DICTIONARY_URL
        resp = requests.get(dictionary_url, stream=True)
        records_saved = 0
        buffer = []
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for chunk in resp.iter_content(chunk_size=512 * 1024):
                if chunk:
                    temp_file.write(chunk)
            resp.close()
        with open(temp_file.name, encoding='utf-8') as temp_file_read:
            for cnt, line in enumerate(temp_file_read):
                buffer.append(line)
                if len(buffer) == BUFFER_LIMIT:
                    save_vesum_lines(buffer)
                    records_saved += len(buffer)
                    logger.warning('lines processed: ' + str(records_saved))
                    buffer.clear()
            if len(buffer) > 0:
                save_vesum_lines(buffer)
                records_saved += len(buffer)
                logger.warning('lines processed: ' + str(records_saved))
        os.remove(temp_file.name)
        client[DB_NAME]['vesum-entry'].ensure_index('word')
        logger.warning('VESUM refresh end')


def save_vesum_lines(lines):
    vesum_entries = list(map(lambda line: parse_vesum_line(line), lines))
    client[DB_NAME]['vesum-entry'].insert_many(vesum_entries)


def parse_vesum_line(line):
    line = line.strip()
    vesum_entry_arr = line.split(' ')
    return {
        'word': vesum_entry_arr[0],
        'mainForm': vesum_entry_arr[1],
        'tags': vesum_entry_arr[2].split(':')
    }


vesum_service = VesumService()
