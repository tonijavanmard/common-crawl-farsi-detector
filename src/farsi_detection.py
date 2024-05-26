import sys
import boto3
import os
import zipfile
import shutil
import logging
import pycld2
from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
import fasttext
import time
import psycopg2

try:
    conn = psycopg2.connect(
    user='postgres', password='11111111', host='database-2.c7g862s2e0k2.us-east-1.rds.amazonaws.com', port= '5432'
    )
except:
    logging.error("Unable to connect to DB.")
    exit()
cursor = conn.cursor()

ceph_bucket_name = 'commoncrawl'
ceph_client = boto3.client('s3')

model = fasttext.load_model('lid.176.bin')

def fetch_segment_file(segment_file_path, save_path):
    ceph_client.download_file(ceph_bucket_name, segment_file_path, save_path)
    logging.info(f"download complete from ceph whit key {segment_file_path}")
    
def uploadFile(localFilePath, cephFilePath):
    ceph_client.upload_file(Filename=localFilePath, Bucket='bashircommoncrawl', Key=cephFilePath)    
    logging.info(f"upload complete on ceph whit key {cephFilePath}")
    
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

def get_segment_data():
    query = "UPDATE segments" + \
        " SET lock_time = NOW(), is_locked = TRUE" + \
        " WHERE segment_id = (SELECT segment_id" + \
        " FROM segments" + \
        " WHERE is_locked = FALSE AND is_finished = FALSE ORDER BY segment_id ASC LIMIT 1)" + \
        " RETURNING segments.*"
    cursor.execute(query)
    conn.commit()

    try:
        collection = cursor.fetchone()
        id = collection[0]
    except:
        conn.close()
        print("Can not fetch segment.")
        time.sleep(8*3600)
        exit()

    if not id:
        conn.close()
        print("Can not fetch segment id.")
        time.sleep(8*3600)
        exit()
    return id, collection[1], collection[2], collection[3]

while True:
    id, segment_name, segment_order, url = get_segment_data()
    print(id, segment_name, segment_order, url)
    segment_file_name = url.split("/")[-1]
    fetch_segment_file(url, segment_file_name)
    indexes = search_for_farsi(segment_file_name)
    store_farsi_warcs(segment_file_name, indexes)
    os.remove(segment_file_name)
    zip_folder(segment_file_name.split(".")[0], f'{segment_file_name.split(".")[0]}.zip')
    shutil.rmtree(segment_file_name.split(".")[0])
    uploadFile(f'{segment_file_name.split(".")[0]}.zip', f'{segment_name}/{segment_file_name.split(".")[0]}.zip')
    os.remove(f'{segment_file_name.split(".")[0]}.zip')
    
    query = f"UPDATE segments SET finish_time = NOW(), is_finished = TRUE, is_locked = FALSE" + \
            f" WHERE segment_id = {id};"

    cursor.execute(query)
    conn.commit()