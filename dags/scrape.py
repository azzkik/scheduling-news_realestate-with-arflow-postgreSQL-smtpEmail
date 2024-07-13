import re
import psycopg2
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from configs import host, database, port, user, password


# Fungsi untuk membuat tabel
def create_table(table_name, columns):
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
                )""")
            conn.commit()

# Fungsi untuk menghapus semua data dari tabel sebelum mengisinya kembali
def truncate_table(table_name):
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()

# Fungsi untuk mengisi tabel dengan data baru
def populate_table(table_name, data):
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.executemany(f"""
                INSERT INTO {table_name} VALUES (
                    {', '.join(['%s'] * len(data[0]))
                })""", data)
            conn.commit()

def scrape_link_db_property(base_url, start_url, max_pages=5):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    
    realestate_link = []
    url = start_url
    page_count = 0 
    
    while url and page_count < max_pages:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            productlist = soup.find_all("div", {"class": "card propertyCard position-relative"})
            
            for product in productlist:
                link = product.find("a", {"class": "btn text-white text-center backgroundColor1 d-block"}).get('href')
                if link:
                    realestate_link.append(link)
            
            # Mencari tombol 'next' untuk pindah ke halaman berikutnya
            next_button = soup.find("a", {"class": "page-link textColorPrimary"})
            if next_button:
                next_url = next_button.get('href')
                url = next_url
            else:
                url = None
                
            
            page_count += 1
            print(f"Halaman {page_count} terscraping")
            
            time.sleep(5)
        
        except requests.exceptions.Timeout:
            print("Request timed out. Sedang mencoba lagi...")
            time.sleep(10)
            continue
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return realestate_link

def scrape_dbrealestate():
    base_url = "https://dbrei.id"
    start_url = "https://dbrei.id/property/list?selectPropertyTypeId=2&selectRegionalId=1&isInsideKomplek=0&select_property_function_id=1&page=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    realestate_link = scrape_link_db_property(base_url, start_url, max_pages=2)

    data_db = []

    for link in realestate_link:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
        }
        f = requests.get(link, headers=headers).text
        hun = BeautifulSoup(f, 'html.parser')

        try:
            price = hun.find("div", {"class": "font-weight-bold mb-3"}).text.replace('\n', "")
        except:
            price = None

        try:
            name = hun.find("h1", {"class": "font-weight-bold"}).text.replace('\n', "")
        except:
            name = None

        tipeproperti = None
        dimensi = None
        image_src = None
        dimensibangunan = None
        tipejual = None
        dimensitanah = None
        lokasiprop = None
        kondisiprop = None
        fungsiprop = None
        orientasi = None
        sertifikat = None
        kaplistrik = None
        lokasi = None
        fasilitas = None
        kamartidur = None
        lantai = None
        kamarmandi = None
        lokasistrat = None

        # Loop through all <div> with class "d-inline-block"
        for div in hun.find_all("div", {"class": "d-inline-block"}):
            title_element = div.find("b", {"class": "d-block title"})
            span_element = div.find("span", {"class": "d-block"})
            
            if title_element and span_element:
                title_text = title_element.text.strip().lower()
                span_text = span_element.text.strip()

                if "tipe properti" in title_text:
                    tipeproperti = span_text
                elif "dimensi bangunan" in title_text:
                    dimensibangunan = span_text
                elif "tipe jual" in title_text:
                    tipejual = span_text
                elif "dimensi tanah" in title_text:
                    dimensitanah = span_text
                elif "lokasi properti" in title_text:
                    lokasiprop = span_text
                elif "kondisi properti" in title_text:
                    kondisiprop = span_text
                elif "fungsi properti" in title_text:
                    fungsiprop = span_text
                elif "orientasi" in title_text:
                    orientasi = span_text
                elif "sertifikat" in title_text:
                    sertifikat = span_text
                elif "kapasitas listrik" in title_text:
                    kaplistrik = span_text
                elif "lokasi strategis" in title_text:
                    lokasistrat = span_text
                elif "lokasi" in title_text:
                    lokasi = span_text
                elif "fasilitas" in title_text:
                    fasilitas = span_text
                elif "kamar tidur" in title_text:
                    kamartidur = span_text
                elif "total lantai" in title_text:
                    lantai = span_text
                elif "kamar mandi" in title_text:
                    kamarmandi = span_text

        try:
            image_src = hun.find("img", {"style": "width: 300px; opacity: 1;"}).get('src')
        except:
            image_src = None

        if image_src is None:
            image_src = "DB Real Estate"
        
        try:
            telepon = hun.find("i", {"class": "fab fa-whatsapp mr-3"}).next_sibling.strip()
        except:
            telepon = None

        try:
            makelar = hun.find("span", {"class": "font-weight-bold"}).text.strip()
        except:
            makelar = None

        house = {
            "Agen Properti": image_src,
            "Judul": name,
            "Lokasi": lokasi,
            "Harga": price,
            "Dimensi Tanah": dimensitanah,
            "Dimensi Bangunan": dimensibangunan,
            "Jumlah Kamar Tidur": kamartidur,
            "Jumlah Kamar Mandi": kamarmandi,
            "Nama Makelar": makelar,
            "Nomor Makelar": telepon
        }
        data_db.append(house)
        
        df_db = pd.DataFrame(data_db)
        df_db = df_db.apply(lambda x: x.str.lower() if x.dtype == 'object' else x)
        df_db['Harga'] = df_db['Harga'].str.replace(',', '.')
        df_db[['Dimensi Tanah', 'Dimensi Bangunan']] = df_db[['Dimensi Tanah', 'Dimensi Bangunan']].replace(r' m²|m²| m2|m2', '', regex=True)
        df_db['Nomor Makelar'] = df_db['Nomor Makelar'].str.replace(r'ind(\d+)', r'0\1', regex=True)
        df_db['Jumlah Kamar Tidur'] = df_db['Jumlah Kamar Tidur'].apply(lambda x: sum(map(int, x.split('+'))) if x else 0)
        df_db['Jumlah Kamar Mandi'] = df_db['Jumlah Kamar Mandi'].apply(lambda x: sum(map(int, x.split('+'))) if x else 0)
        df_db['Nomor Makelar'] = df_db['Nomor Makelar'].str.replace('indonesia', '', regex=False)
        df_db['Nomor Makelar'] = df_db['Nomor Makelar'].str.replace(r'\+62|-|\s', '', regex=True)
        df_db["Dimensi Bangunan"] = df_db["Dimensi Bangunan"].replace("-","0")
        df_db["Dimensi Tanah"] = df_db["Dimensi Tanah"].replace("-","0")
        df_db["Harga"] = df_db["Harga"].str.replace(".","")
        df_db["Jumlah Kamar Tidur"] = df_db["Jumlah Kamar Tidur"].replace("-","0")
        df_db["Jumlah Kamar Mandi"] = df_db["Jumlah Kamar Mandi"].replace("-","0")
        df_db['Nomor Makelar'] = df_db['Nomor Makelar'].apply(lambda x: '0' + x if x.startswith('8') else x)
        realestate_data = [tuple(map(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x, x)) for x in df_db.to_records(index=False)]
    
    create_table("db_real_estate", ["agen_properti VARCHAR(255)", 
                                        "judul VARCHAR(255)", 
                                        "lokasi VARCHAR(255)", 
                                        "harga VARCHAR(255)", 
                                        "dimensi_tanah VARCHAR(255)", 
                                        "dimensi_bangunan VARCHAR(255)", 
                                        "jumlah_kamar_tidur VARCHAR(255)", 
                                        "jumlah_kamar_mandi VARCHAR(255)", 
                                        "nama_makelar VARCHAR(255)", 
                                        "nomor_makelar VARCHAR(255)" ])
        
    truncate_table("db_real_estate")
    populate_table("db_real_estate", realestate_data)


# Winston
def scrape_link_winston_property(base_url, start_url, max_pages=5):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    
    winston_link = []
    url = start_url
    page_count = 0 
    
    while url and page_count < max_pages:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            productlist = soup.find_all("div", {"class": "col-sm-4"})
            
            for product in productlist:
                link_element = product.find("a", {"class": "card breaking"})
                if link_element is not None:
                    link = link_element.get('href')
                    full_link = urljoin(base_url, link)
                    winston_link.append(full_link)
            
            # Menemukan tombol 'next' untuk pindah ke halaman berikutnya menggunakan regex
            next_button = soup.find("a", text=re.compile(r'\bNext\b', re.IGNORECASE))
            if next_button:
                next_url = next_button.get('href')
                url = urljoin(base_url, next_url)
            else:
                url = None
                
            page_count += 1
            print(f"Halaman {page_count} terscraping")
            time.sleep(5)
        
        except requests.exceptions.Timeout:
            print("Request timed out. Sedang mencoba lagi...")
            time.sleep(10)
            continue
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return winston_link

def scrape_winston():
    base_url = "https://www.winston.co.id"
    start_url = "https://www.winston.co.id/search-property/secondary/?Search=1&TypeID=1&JenisListingID=1&CityID=6&page=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    winston_link = scrape_link_winston_property(base_url, start_url, max_pages=2)

    data = []

    for link in winston_link:
        try:
            f = requests.get(link, headers=headers).text
            hun = BeautifulSoup(f, 'html.parser')

            li_elements = hun.find_all("li")
            price = None
            for li in li_elements:
                if "Harga Jual" in li.get_text():
                    price_span = li.find("span")
                    if price_span:
                        price = price_span.get_text().strip().replace('\n', "").replace(",", "").replace("Rp", "").strip()
                        break

            try:
                name = None
                for li in li_elements:
                    if "Info Tambahan" in li.get_text():
                        name_span = li.find("span")
                        if name_span:
                            name = name_span.get_text().strip().replace('\n', "").split("<br>")[0].strip()
                            break
            except Exception as e:
                print(f"Failed to fetch name for {link}: {e}")
                name = None

            try:
                agent_name_div = hun.find("h4", {"class": "judul nama_agent"})
                orang = agent_name_div.get_text().strip() if agent_name_div else None
            except Exception as e:
                print(f"Failed to fetch agent name for {link}: {e}")
                orang = None
            
            phone = None
            telp = hun.find(text=lambda text: text and 'Telp' in text)
            if telp:
                phone = telp.split(':')[-1].strip()

        
            agenproperti = None
            alamat = None
            luastanah = None
            luasbangunan = None
            kamartidur = None
            kamarmandi = None

            # Loop melalui semua <li> dan mengambil informasi spesifik
            for li in li_elements:
                text = li.get_text()
                span = li.find("span")
                if not span:
                    continue

                if "Alamat" in text:
                    alamat = span.get_text().strip()
                elif "Luas Tanah / Luas Bangunan" in text:
                    luas_tanah_bangunan = span.get_text().strip().split("/")
                    if len(luas_tanah_bangunan) == 2:
                        luastanah, luasbangunan = [x.strip() for x in luas_tanah_bangunan]
                elif "Kamar Tidur" in text:
                    kamartidur = span.get_text().strip()
                elif "Kamar Mandi" in text:
                    kamarmandi = span.get_text().strip()

            house = {
                "Agen Properti": None,
                "Judul": name,
                "Lokasi": alamat, 
                "Harga": price, 
                "Dimensi Tanah": luastanah, 
                "Dimensi Bangunan": luasbangunan,
                "Jumlah Kamar Tidur": kamartidur, 
                "Jumlah Kamar Mandi": kamarmandi,
                "Nama Makelar": orang,
                "Nomor Makelar": phone
            }
            data.append(house)
        except Exception as e:
            print(f"Failed to fetch data for {link}: {e}")

        df_winson = pd.DataFrame(data)
        df_winson["Agen Properti"].fillna("Winston Estate", inplace=True)
        df_winson["Judul"].fillna("dijual", inplace=True)
        df_winson = df_winson.apply(lambda x: x.str.lower() if x.dtype == 'object' else x)
        df_winson = df_winson[df_winson["Nama Makelar"] != "tomy"]
        df_winson = df_winson.applymap(lambda x: x.strip())
        df_winson[['Dimensi Tanah', 'Dimensi Bangunan']] = df_winson[['Dimensi Tanah', 'Dimensi Bangunan']].replace(r' m²|m²| m2|m2', '', regex=True)
        df_winson[['Dimensi Tanah', 'Dimensi Bangunan']] = df_winson[['Dimensi Tanah', 'Dimensi Bangunan']].replace(r'^±', '', regex=True)
        df_winson['Nomor Makelar'] = df_winson['Nomor Makelar'].str.replace(r'^\+62', '0', regex=True)
        df_winson['Nomor Makelar'] = df_winson['Nomor Makelar'].apply(lambda x: str(x)[:12])
        df_winson['Nomor Makelar'] = df_winson['Nomor Makelar'].str.replace('-', '')
        df_winson["Dimensi Bangunan"] = df_winson["Dimensi Bangunan"].replace("-","0")
        df_winson["Dimensi Tanah"] = df_winson["Dimensi Tanah"].replace("-","0")
        df_winson["Jumlah Kamar Tidur"] = df_winson["Jumlah Kamar Tidur"].replace("-","0")
        df_winson["Jumlah Kamar Mandi"] = df_winson["Jumlah Kamar Mandi"].replace("-","0")
        remove_words = ['gudang', 'driver']
        pat = '|'.join([r'\b{}\b'.format(w) for w in remove_words])
        df_winson['Jumlah Kamar Tidur'] = df_winson['Jumlah Kamar Tidur'].replace({pat: ''}, regex=True)
        df_winson['Jumlah Kamar Mandi'] = df_winson['Jumlah Kamar Mandi'].replace({pat: ''}, regex=True)
        df_winson['Jumlah Kamar Tidur'] = df_winson['Jumlah Kamar Tidur'].apply(lambda x: sum(map(int, x.split('+'))))
        df_winson['Jumlah Kamar Mandi'] = df_winson['Jumlah Kamar Mandi'].apply(lambda x: sum(map(int, x.split('+'))))
        df_winson.dropna(inplace=True)
        winston_data = [tuple(map(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x, x)) for x in df_winson.to_records(index=False)]

    create_table("db_winston", ["agen_properti VARCHAR(255)", 
                                     "judul VARCHAR(255)", 
                                     "lokasi VARCHAR(255)", 
                                     "harga VARCHAR(255)", 
                                     "dimensi_tanah VARCHAR(255)", 
                                     "dimensi_bangunan VARCHAR(255)", 
                                     "jumlah_kamar_tidur VARCHAR(255)", 
                                     "jumlah_kamar_mandi VARCHAR(255)", 
                                     "nama_makelar VARCHAR(255)", 
                                     "nomor_makelar VARCHAR(255)" ])
    
    truncate_table("db_winston")
    populate_table("db_winston", winston_data)



def scrape_link_xavier_property(base_url, start_url, max_pages=200):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    
    xavier_link = []
    url = start_url
    page_count = 0  
    
    while url and page_count < max_pages:
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            productlist = soup.find_all("div", {"class": "listing-item"})
            
            for product in productlist:
                link = product.find("a", {"class": "geodir-category-img-wrap fl-wrap"}).get('href')
                if link:
                    xavier_link.append(base_url + link)
            
            next_button = soup.find("a", {"data-page": str(page_count + 2)})  
            if next_button:
                next_url = next_button.get('href')
                if not next_url.startswith('http'):
                    next_url = base_url + next_url.lstrip('/')  
                
                url = next_url
            else:
                url = None
                
            page_count += 1
            print(f"Halaman {page_count} terscraping")
            
            time.sleep(5)
        
        except requests.exceptions.Timeout:
            print("Request timed out. Trying again...")
            time.sleep(10)
            continue
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return xavier_link

def scrape_xavier():
    base_url = "https://www.xaviermarks.com/"
    start_url = "https://www.xaviermarks.com/property-search/?key=surabaya+timur&page=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    xavier_link = scrape_link_xavier_property(base_url, start_url, max_pages=2)

    data = []

    for link in xavier_link:
        f = requests.get(link, headers=headers).text
        hun = BeautifulSoup(f,'html.parser')

        try:
            price_div = hun.find("div", {"class": "listing-rating-count-wrap single-list-count"})
            harga = price_div.find("h2").text.strip().replace('\n', "") if price_div else None
        except:
            harga = None

        try:
            name_div = hun.find("div", {"class": "col-md-7"})
            name = name_div.find("h1").text.strip().replace('\n', "") if name_div else None
        except:
            name = None

        tipeproperti = None
        dimensi = None

        # Loop through all <div> with class "d-inline-block"
        for div in hun.find_all("li", {"class": "flex-deskripsi"}):
            title_element = div.find("span", {"class": "deskripsi-item"})
            span_element = div.find("a", {"class": "deskripsi-value"})
            
            if title_element and span_element:
                title_text = title_element.text.strip().lower()
                span_text = span_element.text.strip()

                if "lokasi" in title_text:
                    lokasi = span_text
                if "luas tanah" in title_text:
                    luastanah = span_text
                if "luas bangunan" in title_text:
                    luasbangunan = span_text
                if "kamar tidur" in title_text:
                    kamartidur  = span_text
                if "kamar mandi utama" in title_text:
                    kamarmandiutama = span_text
                if "lokasi" in title_text:
                    lokasi = span_text

        email_element = hun.find("a", href=lambda href: href and "mailto:" in href)
        email = email_element.text.strip() if email_element else None

        agent_name = None
        agent_div = hun.find("div", {"class": "box-widget-author-title_content"})
        if agent_div:
            agent_link = agent_div.find("a", href=lambda href: href and "https://www.xaviermarks.com/" in href)
            if agent_link:
                agent_name = agent_link.text.strip()

        phone_number = None
        phone_a = hun.find("a", href=lambda href: href and href.startswith("tel:"))
        if phone_a:
            phone_number = phone_a.text.strip()

        house = {"Agen Properti" : email,
                "Judul": name,
                "Lokasi": lokasi, 
                "Harga": harga, 
                "Dimensi Tanah": luastanah, 
                "Dimensi Bangunan": luasbangunan,
                "Jumlah Kamar Tidur": kamartidur, 
                "Jumlah Kamar Mandi": kamarmandiutama,
                "Nama Makelar": agent_name,
                "Nomor Makelar": phone_number
                }
        data.append(house)

        df_xavier = pd.DataFrame(data)
        df_xavier = df_xavier.applymap(lambda x: x.lower() if isinstance(x, str) else x)
        df_xavier = df_xavier.replace("office@xaviermarks.com", "xavier property")
        df_xavier["Lokasi"] = df_xavier["Lokasi"].str.replace("\n", " ")
        df_xavier["Lokasi"] = df_xavier["Lokasi"].str.replace("jawa timur, ", "jawa timur")
        df_xavier["Harga"] = df_xavier["Harga"].str.replace("idr ", "")
        df_xavier["Harga"] = df_xavier["Harga"].str.replace(",", "")
        df_xavier["Dimensi Tanah"] = df_xavier["Dimensi Tanah"].str.replace(" m2", "")
        df_xavier["Dimensi Bangunan"] = df_xavier["Dimensi Bangunan"].str.replace(" m2", "")
        df_xavier["Jumlah Kamar Tidur"] = df_xavier["Jumlah Kamar Tidur"].replace("-", "0")
        df_xavier["Jumlah Kamar Mandi"] = df_xavier["Jumlah Kamar Mandi"].replace("-", "0")
        df_xavier["Dimensi Bangunan"] = df_xavier["Dimensi Bangunan"].replace("-","0")
        df_xavier["Dimensi Tanah"] = df_xavier["Dimensi Tanah"].replace("-","0")
        xavier_data = [tuple(map(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x, x)) for x in df_xavier.to_records(index=False)]

    create_table("db_xavier", ["agen_properti VARCHAR(255)", 
                                        "judul VARCHAR(255)", 
                                        "lokasi VARCHAR(255)", 
                                        "harga VARCHAR(255)", 
                                        "dimensi_tanah VARCHAR(255)", 
                                        "dimensi_bangunan VARCHAR(255)", 
                                        "jumlah_kamar_tidur VARCHAR(255)", 
                                        "jumlah_kamar_mandi VARCHAR(255)", 
                                        "nama_makelar VARCHAR(255)", 
                                        "nomor_makelar VARCHAR(255)" ])

    truncate_table("db_xavier")
    populate_table("db_xavier", xavier_data)