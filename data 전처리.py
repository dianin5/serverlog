import pymysql.cursors
import socket
import struct
from datetime import datetime
import sys

PHASES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "dngusdn12",
    "database": "ServerLog"
}

SQL_INSERT_QUERY = """
    INSERT INTO log (date_added, employee_number, source_ip, source_mac, source_port, server_name, destination_ip, destination_mac, destination_port, size_kb)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def ip_to_int(ip_address):
    return struct.unpack("!I", socket.inet_aton(ip_address))[0]

def mac_to_int(mac_address):
    mac_hex = mac_address.replace(':', '').replace('-', '')
    return int(mac_hex, 16)

def convert_to_mysql_datetime(date_str):
    date_str = date_str.replace('\ufeff', '')
    dt = datetime.strptime(date_str, "%b %d %Y %A %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def process_log_line(line):
    raw = line.split()
    date_added_mysql = convert_to_mysql_datetime(' '.join(raw[0:5]))
    employee_number = ' '.join(raw[6:7])
    source_ip = ip_to_int(' '.join(raw[7:8]))
    source_mac = mac_to_int(' '.join(raw[8:9]))
    source_port = ' '.join(raw[9:10])
    server_name = ' '.join(raw[10:11])
    destination_ip = ip_to_int(' '.join(raw[11:12]))
    destination_mac = mac_to_int(' '.join(raw[12:13]))
    destination_port = ' '.join(raw[13:14])
    size_kb = ' '.join(raw[14:15])
    return date_added_mysql, employee_number, source_ip, source_mac, source_port, server_name, destination_ip, destination_mac, destination_port, size_kb

def read_file_and_insert_data(file_path):
    char_list = []
    data_to_insert = []
    ctr = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        while True: 
            char = file.read(1)  
            if not char:  
                break
            char_list.append(char)

            if ''.join(char_list[-3:]) in PHASES and len(char_list) > 100:
                line = ''.join(char_list[:-3])
                data_to_insert.append(process_log_line(line))
                char_list = [''.join(char_list[-3:])]

                if len(data_to_insert) >= 50000:
                    insert_data_to_db(data_to_insert)
                    ctr+=1
                    data_to_insert = []
            if ctr == 10000:
                print(f"Inserted {ctr} records")
        if data_to_insert:
            insert_data_to_db(data_to_insert)

def insert_data_to_db(data_to_insert):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.executemany(SQL_INSERT_QUERY, data_to_insert)
        conn.commit()
    except pymysql.Error as e:
        print("Error while inserting data:", e)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_path = 'security.log'
    read_file_and_insert_data(file_path)
