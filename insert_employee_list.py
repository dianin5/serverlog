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
        return int(socket.inet_aton(ip_address).hex(), 16)

    def convert_mac_to_int(self, mac_address):
        mac_hex = mac_address.replace(':', '').replace('-', '')
        return int(mac_hex, 16)

    def execute_insert(self, table_name, data):
        query = "INSERT INTO employee_list (No, NAME, ACCOUNT, IP, MAC, REGION) VALUES (%s, %s, %s, %s, %s, %s)"
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
        name = row['NAME']  # 'Name'을 'NAME'으로 변경
        account = row['ACCOUNT']
        ip = db_handler.convert_ip_to_int(str(row['IP']))
        mac = db_handler.convert_mac_to_int(str(row['MAC']))
        region = row['REGION']
        db_handler.execute_insert("employee_list", (num, name, account, ip, mac, region))
        print(index, num, name, account, ip, mac, region)
    db_handler.commit()

if __name__ == "__main__":
    csv_file_path = "EMPLOYEE_LIST.csv"

    db_host = "localhost"
    db_user = "root"
    db_password = "dngusdn12"
    db_name = "ServerLog"

    db_handler = DatabaseHandler(db_host, db_user, db_password, db_name)
    read_and_insert_csv(csv_file_path, db_handler)
    db_handler.close()