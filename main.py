from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException
from psycopg2.extras import execute_values
import psycopg2
import time


def main():

    #Uncomment the line below if you want to use with Firefox, and comment the lines for Chrome (14 - 16).
    #driver = webdriver.Firefox()

    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options)
    
    driver.get("https://coinmarketcap.com/")

    for i in range(15):
        driver.find_element(By.CSS_SELECTOR, 'body').send_keys(Keys.PAGE_DOWN)

    table_tr_xpath = '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div[1]/div[4]/table/tbody/tr'
    tr_list = driver.find_elements(By.XPATH, table_tr_xpath)


    coin_list = []
    for tr in tr_list:
        coin_dict = {}
        try:
            all_tds = tr.find_elements(By.TAG_NAME, 'td')

            coin_dict['id'] = all_tds[1].text
            coin_dict['Name'] = all_tds[2].find_element(By.CSS_SELECTOR, 'p[color="text"]').text
            coin_dict['Symbol'] = all_tds[2].find_element(By.CSS_SELECTOR, 'p[color="text3"]').text
            price_usd = float(all_tds[3].text.replace('$', '').replace(',', '').replace('...', '').strip())

            coin_dict['Price_USD'] = float(price_usd)

            price_brl = round(price_usd * 5, 5)
            coin_dict['Price_BRL'] = float(price_brl)

            coin_dict['Market_cap_USD'] = float(all_tds[7].text.replace('$', '').replace(',', '').replace('...', '').strip())

            coin_dict['Volume_24h_USD'] = int(all_tds[8].find_element(By.CSS_SELECTOR, 'p[color="text"]').text.replace('$', '').replace(',', '').replace('...', '').strip())

            circulating_supply = all_tds[9].text.split()
            circulating_number = circulating_supply[0].replace(',', '')
            coin_dict['Circulating_Supply'] = int(circulating_number)

        except StaleElementReferenceException as Exception:
            pass
        coin_list.append(coin_dict)
    
    time.sleep(3)
    driver.close()
    coins_tuples = [tuple(dict.values()) for dict in coin_list]

    #Connect with your db
    conn = psycopg2.connect(host='localhost', dbname='cryptodb', user='postgres', password='', port='5432')

    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS crypto (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        price_usd FLOAT,
        price_brl_round FLOAT,
        market_cap_usd BIGINT,
        volume_24h_usd BIGINT,
        circulating_supply BIGINT
    );
    ''')

    execute_values(cur, 
                   "INSERT INTO crypto (id, name, symbol, price_usd, price_brl_round, market_cap_usd, volume_24h_usd, circulating_supply) VALUES %s", coins_tuples)

    conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
