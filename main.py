import requests
import schedule
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from datetime import datetime
from decimal import Decimal
import os


URL = ''

CONFIG ={
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASS'),
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'raise_on_warnings': True,
    'auth_plugin': 'mysql_native_password'
}

def get_product_from_api(info = None):
    if info == None:
        return 404
    else:
        try:
            response = requests.get(URL+info[0]+'?attrs=sku,listPrice')
            if response.status_code == 200:
                r = response.json()
                product = (r['listPrice']['value'], info[1], r['sku'])
                return product
            elif response.status_code == 404:
                product = (99999999, info[1], info[0])
                return product
            else:
                pass
        except:
            pass
        
def get_products_from_database():
    db_name = CONFIG['database']
    query = f"""SELECT 
                ean, price
                FROM {db_name}.Product 
                where ean is not null
                and ean != ''
                LIMIT %s, %s
            """
        
    cnx = mysql.connector.connect(**CONFIG)
    cursor = cnx.cursor()
    cursor.execute(query, (1, 1))
    return list(cursor.fetchall())

def main():
    product_list = get_products_from_database()

    lock = threading.Lock()
    timestamp = time.time()
    with open("data/pricechanges-{}.txt".format(timestamp), "w") as file:
        resultList = []
        with ThreadPoolExecutor(max_workers=20) as pool:
            results = pool.map(get_product_from_api, product_list)
            for result in results:
                if result != None:
                    if Decimal(result[0]).quantize(Decimal('.01')) != Decimal(result[1]).quantize(Decimal('.01')):
                        resultList.append(result)
                        str = f"{result[0]}, {result[1]}, {result[2]}\n"
                        with lock:
                            file.write(str)
        db_name = CONFIG['database']
        query = f"""
                    UPDATE `{db_name}`.`Product` 
                    SET `price` = %s, 
                        `previous_price` = %s 
                    WHERE `ean` = %s; 
                """
        cnx = mysql.connector.connect(**CONFIG)
        cursor = cnx.cursor()
        cursor.executemany(query, resultList)
        cnx.commit()
        cursor.close()


def run():
    start_time = time.time()
    print(f"Starting at {datetime.now()}")
    main()
    print(f"Finished after {(time.time() - start_time):.2f} seconds at {datetime.now()}")

schedule.every(8).hours.do(run)    
while True:
    schedule.run_pending()
    time.sleep(1)
