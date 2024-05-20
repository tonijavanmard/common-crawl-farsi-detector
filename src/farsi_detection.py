import sys
import sh
import os
import zipfile
import shutil
import logging
import pycld2
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
import fasttext

SEGMENTS_FILE_NAME = sys.argv[1] + ".txt"
model = fasttext.load_model('lid.176.bin')

def fetch_segment_file(segment_file_path):
    s3 = sh.bash.bake("aws s3")
    s3.put("cp", f"s3://commoncrawl{segment_file_path}", ".")
    
def is_farsi(payload):
    result = pycld2.detect(payload)
    return result[2][0][0] == 'PERSIAN' or result[2][1][0] == 'PERSIAN' or result[2][2][0] == 'PERSIAN'

def is_farsi_level2(text):
    try:
        predictions = model.predict(text, k=1)  
        return predictions[0][0] == '__label__fa'
    except:
        return False

def search_for_farsi(warc_address):
    parts = set()
    counter = -1
    with open(warc_address, 'rb') as stream:
        for record in ArchiveIterator(stream):
            counter = counter + 1
            if str(record.rec_headers.get_header('WARC-Type')) != 'response':
                continue
            payload = record.content_stream().read()
            payload_str = payload.decode("utf-8", errors="ignore")
            payload_bytes = payload_str.encode()
            try:
                if is_farsi(payload_bytes):
                    parts.add(counter)
                    logging.info('Detect a farsi warc with cld2.')
            except:
                if is_farsi_level2(payload_str):
                    parts.add(counter)
                    logging.info('Detect a farsi warc with fasttext.')
                else:
                    logging.info('There is problem to farsi detection.')
    return parts

def store_farsi_warcs(warc_address, indexes):
    folder = warc_address.split(".")[0]
    os.makedirs(folder, exist_ok=True)
    counter = -1
    with open(warc_address, 'rb') as stream:
        for record in ArchiveIterator(stream):
            counter = counter + 1
            if counter in indexes:
                digest = str(record.rec_headers.get_header('WARC-Payload-Digest'))[5:]
                with open(f'{folder}/{digest}.warc.gz', 'wb') as output_file:
                    writer = WARCWriter(output_file)
                    writer.write_record(record)
                    

def zip_folder(folder_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, folder_path))

with open(SEGMENTS_FILE_NAME, 'r') as file:
    for line in file:
        segment_file_path = line.strip()
        segment_file_name = segment_file_path.split("/")[-1]
        fetch_segment_file(segment_file_path)
        indexes = search_for_farsi(segment_file_name)
        store_farsi_warcs(segment_file_name, indexes)
        os.remove(segment_file_name)
        zip_folder(segment_file_name.split(".")[0], f'{segment_file_name.split(".")[0]}.zip')
        shutil.rmtree(segment_file_name.split(".")[0])
