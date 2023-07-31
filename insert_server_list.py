import pandas as pd
import mysql.connector
import socket
import struct

class DatabaseHandler:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def convert_ip_to_int(self, ip_address):
        return struct.unpack("!I", socket.inet_aton(ip_address))[0]

    def convert_mac_to_int(self, mac_address):
        mac_hex = mac_address.replace(':', '').replace('-', '')
        return int(mac_hex, 16)

    def execute_insert(self, data):
        query = "INSERT INTO server_list (No, Name, IP, MAC, PORT) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.execute(query, data)

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

def read_and_insert_csv(csv_file_path, db_handler):
    df = pd.read_csv(csv_file_path)
    print(df)
    for index, row in df.iterrows():
        if index >= 500: break
        num = str(row['No']).replace(',', '')
        name = row['Name']
        ip = db_handler.convert_ip_to_int(str(row['IP']))
        mac = db_handler.convert_mac_to_int(str(row['MAC']))
        port = row['PORT']
        db_handler.execute_insert((num, name, ip, mac, port))
        print(index, num, name, ip, mac, port)
    db_handler.commit()

if __name__ == "__main__":
    csv_file_path = "SERVER_LIST.csv"

    db_host = "localhost"
    db_user = "root"
    db_password = "dngusdn12"
    db_name = "ServerLog"

    db_handler = DatabaseHandler(db_host, db_user, db_password, db_name)

    read_and_insert_csv(csv_file_path, db_handler)

    db_handler.close()