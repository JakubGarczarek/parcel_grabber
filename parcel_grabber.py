import requests, re, json, csv, datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

class ParcelGrabber():

    def __init__(self, plik_csv):

        self.plik_csv = plik_csv
        # ten słownik będzie miał taki układ:
        # {LOKALIZACJA,[teryt1, teryt2, ...]}
        self.lok_teryts = {}
        with open(self.plik_csv) as f:
            csv_cont = csv.reader(f, delimiter=',')
            # dodanie do słownika wszystkich lokalizacji
            # jako klucze i pustych na razie list jako ich wartości
            # docelowo w tych listach będzie zestaw terytów
            for row in csv_cont:
                lokalizacja = row[0]
                self.lok_teryts[lokalizacja]=[]
        # ponowne otwarcie tego samego pliku 
        # żeby od początku przejechać po wierszach csv
        # tym razem w celu uzupełnienia pustych list w słowniku terytami
        with open(self.plik_csv) as f:
            csv_cont = csv.reader(f, delimiter=',')
            for row in csv_cont:
                lokalizacja = row[0]
                teryt = row[1]
                self.lok_teryts[lokalizacja].append(teryt)            
        # sama nazwa bez roszerzenia
        self.nazwa_csv = plik_csv[0:-4]
        with open('config/connection.json') as f:
            connection_json = json.load(f)
            user = connection_json['user']
            password = connection_json['password']
            ip = connection_json['ip']
            port = connection_json['port']
            db = connection_json['db']
        # automatyczne podłączenie bazy z configa
        self.postgis = create_engine(f"postgresql://{user}:{password}@{ip}:{port}/{db}")
    

    ###################################################
    # CSV (lista TERYTÓW) => JSON {"TERYT":"GEOM_WKT"}
    ###################################################
    def geom_from_uldk(self):
        # narazie pusty słownik {"TERYT":"GEOM_WKT"}
        teryt_geom = {}
        # i jeszcze jeden, ale z lokalizacją {"LOKALIZACJA":{"TERYT":"GEOM_WKT"}}
        lok_teryt_geom = {}
        for lokalizacja in self.lok_teryts.keys():
            for teryt in self.lok_teryts[lokalizacja]:
                print(teryt)
        # iteracja po wszystkich lokalizacjach słownika
        for lokalizacja in self.lok_teryts.keys():
            # iteracja po wszystkich terytach w danej lokalizacji
            for teryt in self.lok_teryts[lokalizacja]:
                # zapytanie do usługi uldk z podaniem iterowanego terytu
                query= f"https://uldk.gugik.gov.pl/?request=GetParcelById&id={teryt}&result=geom_wkt"
                print(query)
                response = requests.get(query)
                # gdy serwer odpowie poprawnie i w contencie zwróci jakieś poligony
                if response.status_code == 200 and re.search('POLYGON\(\((.+?)\)\)', (str(response.content))): 
                    # serwer zwraca wkt z białymi znakami
                    wkt_uncleaned = str(response.content)
                    # czyścimy 
                    # wyciągnięcie samych współrzędnych
                    only_xy = re.search('POLYGON\(\((.+?)\)\)', wkt_uncleaned).group(1) 
                    # ponowne opakowanie ich w POLYGON(()) wg formatu wkt
                    geom = f"POLYGON(({only_xy}))"
                    # dodanie do słownika pojedynczej pary {"TERYT":"GEOM_WKT"}
                    teryt_geom[teryt] = geom
                    print('ok ')
                # gdy brak odpowiedzi z serwera lub content pusty
                else:
                    # dopisz do pliku z błędami lokalizację i teryt
                    # którego geometri nie udało się pobrać
                   print('lipa ')
                   with open(f"uldk_braki.csv", "a",encoding = 'utf-8') as f:
                        linia_bledu = csv.writer(f, delimiter=',')
                        linia_bledu.writerow([lokalizacja,teryt,datetime.datetime.now()])    
                
            # zapis do słownika {"TERYT":"GEOM_WKT"} dla całej iterowanej lokalizacji
            lok_teryt_geom[lokalizacja]=teryt_geom
            print(lok_teryt_geom) 
            # zapis tego słownika do json
            with open ('uldk.json', 'w', encoding='utf-8') as f:
                json.dump(lok_teryt_geom, f, indent=1)
            # usunięcie terytu z przyporządkowanymi geometriami
            # pod kolejną iterację lokalizacji
            teryt_geom = {}


    ##############################################################################
    # teryt & geometria z ULDK do postgis 
    ###############################################################################

    def uldk_json_to_postgis(self):
        with open('uldk.json') as f:
            uldk_json = json.load(f)
            for lokalizacja, ter_geoms in uldk_json.items():
                for teryt, geom_92 in ter_geoms.items():
                    with open('wfs_params.json') as w:
                        wfs_params_json=json.load(w)
                        for lokaliz, wfs_params in wfs_params_json.items():
                            if lokaliz == lokalizacja:
                                nr_ukladu = wfs_params['srsname'][-4:]
                    geometrie_lokalne = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{geom_92}',2180),{nr_ukladu}))")
                    for geom_loc in geometrie_lokalne:
                        sql = f"INSERT INTO uldk VALUES ('{lokalizacja}','{teryt}','{geom_92}','{geom_loc[0]}')"
                    self.postgis.execute(sql)
                    print(sql)     

                
   #################################################################################################
   # pozyskanie bboxa z tabeli postgis przy użyciu ST_Extend i zapis do jsona dla każdej lokalizacji 
   #################################################################################################
    
    def bbox_from_postgis(self):
        lokalizacja_bbox_92 = {}
        lokalizacja_bbox_loc = {}
        with open('uldk.json') as f:
            uldk_json = json.load(f)
            for lokalizacja in uldk_json.keys():

                sql_92 = f"SELECT ST_Extent(geom_92) as bextent FROM uldk WHERE lokalizacja='{lokalizacja}'"
                print(sql_92)
                result=self.postgis.execute(sql_92)
                for i in result:
                    if i[0]!= None:
                        extracted_digits_from_bbox =  re.findall("\d+\.\d+",i[0])
                        print(extracted_digits_from_bbox)
                        x_min = extracted_digits_from_bbox[0]
                        y_min = extracted_digits_from_bbox[1]
                        x_max = extracted_digits_from_bbox[2]
                        y_max = extracted_digits_from_bbox[3]
                        bbox_from_postgis = f"{y_min},{x_min},{y_max},{x_max}"
                        print(bbox_from_postgis)
                        lokalizacja_bbox_92[lokalizacja] = bbox_from_postgis

                sql_loc = f"SELECT ST_Extent(geom_loc) as bextent FROM uldk WHERE lokalizacja='{lokalizacja}'"
                print(sql_loc)
                result=self.postgis.execute(sql_loc)
                for i in result:
                    if i[0]!= None:
                        extracted_digits_from_bbox =  re.findall("\d+\.\d+",i[0])
                        print(extracted_digits_from_bbox)
                        x_min = extracted_digits_from_bbox[0]
                        y_min = extracted_digits_from_bbox[1]
                        x_max = extracted_digits_from_bbox[2]
                        y_max = extracted_digits_from_bbox[3]
                        bbox_from_postgis = f"{y_min},{x_min},{y_max},{x_max}"
                        print(bbox_from_postgis)
                        lokalizacja_bbox_loc[lokalizacja] = bbox_from_postgis
                        
                                             
        with open('bbox_92.json','w') as f:
            json.dump(lokalizacja_bbox_92,f,indent=1)
        with open('bbox_loc.json','w') as f:
            json.dump(lokalizacja_bbox_loc,f,indent=1)
        
    
    ###########################################
    # CSV + ORGANY.JSON => URL, TYPENAME, EPSG
    ###########################################

    def wfs_params(self):
        # pusty słownik na parametry zapytania wfs
        wfs_params = {}
        with open('uldk.json') as j:
            uldk_json = json.load(j)
            for lokalizacja, dane in uldk_json.items():
                for teryt, geometria in dane.items():
                    # lista do wrzucania wystąpień danego terytu
                    licz_teryty = []
                    # pobranie 4 pierwszych cyfr terytu
                    teryt_powiatu = teryt[:4]
                    licz_teryty.append(teryt_powiatu)
                    # najczęściej występujący teryt
                    best_teryt = max(licz_teryty, key=licz_teryty.count)
                    # pobranie danych z jsona przygotowanego
                    # jednorazowo przez json_exporter.py
                    with open ('./config/wfs_param.json') as f:
                        wfs_param_json = json.load(f)
                        wfs_url = ''
                        wfs_typename = ''
                        wfs_srsname = ''
                        # pusty narazie pod-słownik na te 3 parametry
                        url_typename_srsname = {}
                        for param in wfs_param_json.values():
                            # porównanie z 4 pierwszymi cyframi terytu działki
                            if best_teryt == param['teryt'][:4]:
                            # wyciągnięcie urla i obcięcie apostrofów
                                wfs_url = param['url'][1:-1]
                            # wyciagnięcie typename
                                wfs_typename = param['typename'][1:-1]
                            # wyciągnięcie układu wsp
                                wfs_srsname = param['srsname'][1:-1]
                            # wypełnienie pod-słownika tymi parametrami
                                url_typename_srsname['url'] = wfs_url
                                url_typename_srsname['typename'] = wfs_typename
                                url_typename_srsname['srsname'] = wfs_srsname
                                wfs_params[lokalizacja] = url_typename_srsname
        with open('wfs_params.json','w') as f:
            json.dump(wfs_params, f, indent=1)
        # return [wfs_url, wfs_typename, wfs_srsname]


     #######################################################################
     # WFS (GML) z podanego bboxa dla danej lub wszystkich  lokalizacji z []
     #######################################################################  

    def wfs_from_bbox(self, location=[]):
        with open('wfs_params.json') as f:
            wfs_params_json = json.load(f)
            for lokalizacja, wfs_params in wfs_params_json.items():
                if lokalizacja in location or location==[]:
                    # właściwy url dla powiatu
                    url = wfs_params['url']
                    # nazwa w-wy z działkami w danym powiecie
                    typename = wfs_params['typename']
                    # w jakim układzie powiat wystawia usługę
                    srsname = wfs_params['srsname']
                    # pobranie wersji wfs:
                    query_cap = f"{url}?SERVICE=WFS&REQUEST=GetCapabilities"
                    print(query_cap)
                    try:
                        response = requests.get(query_cap)
                        if response.status_code == 200:
                            # zapis xmla z capabilities
                            capabilities = response.content
                            print('ok')
                            print(capabilities)
                            cap_soup = BeautifulSoup(capabilities,'xml')
                            wfs_version = cap_soup.find('ows:ServiceTypeVersion').text
                            print(wfs_version)
                        else:
                            # zapis zapytania url, czasu i bboxa które zwróciły błąd
                            print('lipa')
                        # pobranie bboxa z ULDK w 1992
                        with open ('bbox_92.json') as f:
                            bbox = json.load(f)[lokalizacja]
                        # jeżeli w innym niż 1992
                        if srsname != 'EPSG:2180':
                            print(f"układ {srsname} !")
                            # wyciągnięcie samych xy bboxa bez przecinków
                            bboxy= re.findall("\d+\.\d+", bbox)
                            # skomponowanie z nich poligonu do transformacji
                            x1, y1, x2, y2 = bboxy[0], bboxy[1], bboxy[2], bboxy[3]
                            # wcześniejsza wersja alternatywna - transformacja poligonu (prostokąta)
                            # powstałego ze współrzędnych bboxa, a nie pojedynczych punktów:
                            # przerobienie 4 wsp bboxa na 
                            # geometrię poligonu, żeby funkcja
                            # ST_Transform mogła ją przyjąć
                            # bbox_poly_92 = f"POLYGON(({x1} {y1}, {x1} {y2}, {x2} {y2}, {x2} {y1}, {x1} {y1}))"
                            # a to wersja z punktami bboxa zamiast polgifonu (prostokąta)
                            point_min = f"POINT({bboxy[1]} {bboxy[0]})"
                            point_max = f"POINT({bboxy[3]} {bboxy[2]})"
                            # transformacja do układu lokalnego
                            # dla wersji z poligonem
                            # bbox_geom_transf = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{bbox_poly_92}',2180),2176))")
                            # dla wersji z punktami
                            nr_ukladu = srsname[-4:]
                            point_min_transform = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{point_min}',2180),{nr_ukladu}))")
                            point_max_transform = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{point_max}',2180),{nr_ukladu}))")
                            # do tej listy wrzucimy 2 punkty bboxa
                            # po transformacji (do ukł lokalnego)
                            pkt_transformed = []
                            for pkt in point_min_transform:
                                min_xy_only = re.findall("\d+\.\d+", pkt[0])
                                for xy in min_xy_only:
                                    pkt_transformed.append(xy)
                            for pkt in point_max_transform:
                                max_xy_only = re.findall("\d+\.\d+", pkt[0])
                                for xy in max_xy_only:
                                    pkt_transformed.append(xy)
                            x1, y1, x2 ,y2 = pkt_transformed[0], pkt_transformed[1], pkt_transformed[2], pkt_transformed[3]
                            # bbox w układzie lokalnym
                            bbox = f"{x1}, {y1}, {x2}, {y2}"
                            print(bbox)
                            # dla wersji z poligonem(porstokątem)
                            # powrót do formy bboxa(zamiast geometrii)
                        #     for geom in bbox_geom_transf:
                        #         # wyciągnięcie samych liczb z geometri
                        #         print(geom)
                        #         geomxy = re.findall("\d+\.\d+",geom[0])
                        #         coords = []
                        #         for coord in geomxy:
                        #             coords.append(coord)
                        #         bbox=f"{coords[1]},{coords[0]},{coords[3]},{coords[4]}"
                            # zapytanie z bboxem w układzie lokalnym
                            query = f"{url}?SERVICE=WFS&REQUEST=GetFeature&version={wfs_version}&TYPENAMES={typename}&bbox={bbox}&SRSNAME={srsname}"
                            print(query)
                            response = requests.get(query)
                            if response.status_code == 200:       
                                with open(f"{lokalizacja}.gml", 'wb') as f:
                                    f.write(response.content)
                                print(response.content)
                                # otwarcie zapisanego przed chwilą gmla
                                with open(f"{lokalizacja}.gml") as f:
                                    # gml jest w ukł lokalnym i ma odwórcone osie
                                    # osie odwracamy funkcją postgisa ST_Affine
                                    # ale nie można do niej wrzucić całego gmla
                                    # tylko pojedyncze znaczniki zawierające gml:Polygon
                                    soup = BeautifulSoup(f,'xml')
                                    # wyciągamy z gmla zawartość tagów <gml:Polygon>
                                    gml_Polygons = soup.findAll('gml:Polygon')
                                    # pomimo iż usługa w układzie lokalnym
                                    # zapytanie może być wymagane nadal w 1992
                                    # wtedy zwrócony gml będzie pusty, więc poniżej
                                    # sprawdzenie czy pusty i zapytanie w 1992
                                    if not gml_Polygons:
                                        with open ('bbox_92.json') as f:
                                            bbox_92 = json.load(f)[lokalizacja]
                                        query = f"{url}?SERVICE=WFS&REQUEST=GetFeature&version={wfs_version}&TYPENAMES={typename}&bbox={bbox_92}&SRSNAME=EPSG:2180"
                                        print(query)
                                        response = requests.get(query)
                                        if response.status_code == 200:
                                            # zapis gmla - jeśli jest w 1992 to ok
                                            with open(f"{lokalizacja}.gml", 'wb') as f:
                                                f.write(response.content)
                                            print('ok')
                                        else:
                                            # zapis zapytania url, czasu i bboxa które zwróciły błąd
                                            with open(f"wfs_braki.csv", "a",encoding = 'utf-8') as f:
                                                linia_bledu = csv.writer(f, delimiter=',')
                                                linia_bledu.writerow([query,datetime.datetime.now(),bbox,])
                                    for gml_Polygon in gml_Polygons:
                                        # TODO: musimy też jakoś złapać teryt dla danego <gml:Polygona>
                                        # ewns_geometria = gml_Polygon.parent
                                        # ewns_ID_DZIALKI = ewns_geometria.find_previous_sibling('ewns:ID_DZIALKI')
                                        # teryt = ewns_ID_DZIALKI.text
                                        
                                        # Zawartość danego <gml:Polygon> do WKB (ST_GeomFromGML)
                                        # WKB na WKT (ST_AsText)
                                        # Transformacja afiniczna ST_Affine (da się tylko z WKT)
                                        # WKB (powst. z afinicznej) na WKT (ST_AsText)
                                        sql =f"SELECT ST_AsText(ST_Affine(ST_AsText(ST_GeomFromGML('{gml_Polygon}')),0, 1, 1, 0, 0, 0))"
                                        print(sql)
                                        gml_geom = self.postgis.execute(sql)
                                        for g in gml_geom:
                                            # zapis geometrii lokalnych  
                                            # wyciągniętych z GMLA
                                            # (trzeba je jeszcze przetransformować do 1992)
                                            with open(f"{lokalizacja}_WFS.csv", 'a') as f:
                                                f.write(f"{g[0]}\n")
                                        print('ok')
                            else:
                                # zapis zapytania url, czasu i bboxa które zwróciły błąd
                                with open(f"wfs_braki.csv", "a",encoding = 'utf-8') as f:
                                    linia_bledu = csv.writer(f, delimiter=',')
                                    linia_bledu.writerow([query,datetime.datetime.now(),bbox,])
                                print('lipa')
                    
                        # jeżeli nie trzeba było robić transformacji
                        # (dla powiatów z układem 1992)
                        # zapis gml bez odwrócenia osi i ponownej transformacji z lokalnego do 1992
                        else:
                            query = f"{url}?SERVICE=WFS&REQUEST=GetFeature&version={wfs_version}&TYPENAMES={typename}&bbox={bbox}&SRSNAME={srsname}"
                            print(query)
                            try:
                                response = requests.get(query)
                            except ConnectionError as e:
                                print(e.message)
                                pass
                            if response.status_code == 200:
                                # zapis gmla - jeśli jest w 1992 to ok
                                with open(f"{lokalizacja}.gml", 'wb') as f:
                                    f.write(response.content)
                                print('ok')
                            else:
                                # zapis zapytania url, czasu i bboxa które zwróciły błąd
                                with open(f"wfs_braki.csv", "a",encoding = 'utf-8') as f:
                                    linia_bledu = csv.writer(f, delimiter=',')
                                    linia_bledu.writerow([query,datetime.datetime.now(),bbox,])
                    except:
                        print('==========================PASS=================================')
                        pass        
        
    ##############################################################################
    # wyciągnięcie z jsona geometrii do csv (gdy trzeba wizualizować np. w QGIS) 
    ###############################################################################

    def uldk_json_to_csv_geom(self):
        with open('uldk.json') as f:
            uldk_json = json.load(f)
            with open('uldk_teryt_geom.csv', 'w') as all:
                teryt_geom_all = csv.writer(all)
                for lokalizacja, ter_geoms in uldk_json.items():
                    for teryt, geom in ter_geoms.items():
                        teryt_geom_all.writerow([lokalizacja, teryt, geom])
       

                    
    ##############################################################################
    # stworzenie mini-bboxów do testów 
    ###############################################################################
    def mini_bbox_92(self, rozmar_metry):
        lok_mini_bbox = {}
        with open('bbox_92.json') as f:
            bbox_json = json.load(f)
            for lokalizacja, bbox in bbox_json.items():
                x_min = bbox.split(',')[0]
                y_min = bbox.split(',')[1]
                x_max = float(x_min) + float(rozmar_metry)
                y_max = float(y_min) + float(rozmar_metry)
                mini_bbox = f"{x_min},{y_min},{x_max},{y_max}"
                lok_mini_bbox[lokalizacja] = mini_bbox
        with open('mini_bbox_92.json', 'w') as f:
            json.dump(lok_mini_bbox, f, indent=1)



pg = ParcelGrabber('robocze/all_lok_teryt.csv')
# pg.geom_from_uldk()
# pg.bbox_92()
# pg.wfs_params()
# pg.wfs_from_bbox(['Dubie'])
# pg.uldk_json_to_csv_geom()
# pg.uldk_json_to_postgis()
# pg.mini_bbox_92(100)
pg.bbox_from_postgis()