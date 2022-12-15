import requests, re, json, csv
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import config
class ParcelGrabber():

    def __init__(self, csv_file):

        self.csv = csv_file
        # ten słownik będzie miał taki układ:
        # {LOKALIZACJA,[teryt1, teryt2, ...]}
        self.lok_teryts = {}
        with open(self.csv) as f:
            csv_cont = csv.reader(f, delimiter=',')
            # dodanie do słownika wszystkich lokalizacji
            # jako klucze i pustych na razie list jako ich wartości
            # docelowo w tych listach będzie zestaw terytów
            for row in csv_cont:
                lokalizacja = row[0]
                self.lok_teryts[lokalizacja]=[]
        with open(self.csv) as f:
            csv_cont = csv.reader(f, delimiter=',')
            for row in csv_cont:
                lokalizacja = row[0]
                teryt = row[1]
                print(lokalizacja, teryt)
                self.lok_teryts[lokalizacja].append(teryt)
        print(self.lok_teryts)

      
        # sama nazwa bez roszerzenia
        self.fname = csv_file[0:-4]
        # automatyczne podłączenie bazy z configa
        self.postgis = create_engine(f"postgresql://{config.user}:{config.password}@{config.ip}:{config.port}/{config.db}")
        

    ###################################################
    # CSV (lista TERYTÓW) => JSON {"TERYT":"GEOM_WKT"}
    ###################################################

    def geom_from_uldk(self):
        # narazie pusty słownik {"TERYT":"GEOM_WKT"}
        teryt_geom = {}
        # otwieramy csv z terytami
        with open(self.csv) as csv:
            for line in csv:
                # pobranie pojedynczego terytu z csv
                teryt = line.strip()
                # zapytanie do usługi uldk z podaniem terytu
                querry= f"https://uldk.gugik.gov.pl/?request=GetParcelById&id={teryt}&result=geom_wkt"
                response = requests.get(querry)
                # gdy serwer odpowie poprawnie i coś zwróci w content
                if response.status_code == 200:
                    # serwer zwraca wkt z białymi znakami
                    wkt_uncleaned = str(response.content)
                    # czyścimy 
                    # wyciągnięcie samych współrzędnych
                    only_xy = re.search('POLYGON\(\((.+?)\)\)', wkt_uncleaned).group(1) 
                    # ponowne opakowanie ich w POLYGON(()) wg formatu wkt
                    geom = f"POLYGON(({only_xy}))"
                    # dodanie do słownika pojedynczej pary {"TERYT":"GEOM_WKT"}
                    teryt_geom[teryt] = geom
                # gdy brak odpowiedzi z serwera lub content pusty
                else:
                    # utwórz plik o nazwie jak csv ale z sufixem "braki" (nadpisanie)
                   with open(f"{self.fname}_brak_ULDK.csv", "w",encoding = 'utf-8') as f:
                    # zapisz w nowej lini teryt którego geometrii serwer nie zwrócił
                        f.write(teryt+"\n")  
                    
        # zapis słownika {"TERYT":"GEOM_WKT"} do pliku JSON (nadpisanie)
        with open (f"{self.fname}.json", "w", encoding='utf-8') as f:
            json.dump(teryt_geom, f)
        # dodatkowo zwrotka finalnego wyniku (nie tylko json)
        return teryt_geom
    


    ####################################
    # JSON {"TERYT":"GEOM_WKT"} => BBOX
    ####################################

    def bbox(self):
        #utworzenie pustej listy porównawczej, 
        # do której później wpadać będą pary kompletów (listy) współrzędnych
        compare_list = []
        #otworzenie jsona stworzonego przez uldk.json()
        with open(f"{self.csv[0:-4]}.json") as f:
            j = json.load(f)
            # iteracja przez wszystkie pary {"TERYT":"GEOM_WKT"} z jsona
            for geom in j.values():
                # wyodrębnienie samych liczb (współrzędnych) z geom (do listy)
                xy_list = re.findall("\d+\.\d+",geom)
                # lista samych x-ów (co 2 element od 0)
                x_list = xy_list[0::2]
                # lista samych y-ków (co 2 element od 1)
                y_list = xy_list[1::2]
                # dodanie ich extremów do utworzonej wcześniej listy porównawczej
                compare_list.append( [min(x_list), min(y_list), max(x_list), max(y_list)] )
                # Jeżeli w tej liście znajduje się aktualnie
                # komplet (para) list współrzędnych do porównania
                # tworzymy z nich jedną listę z ekstremami 
                if len(compare_list) == 2:
                    # pobranie kompletów ekstremów do zmiennych a i b
                    # a oraz b to listy o strukturze [min x, min y, max x, max y]
                    a = compare_list[0]
                    b = compare_list[1]
                    # najmniejszy x min (pomiędzy a i b)
                    ab_min_x = min(a[0], b[0])
                    # najmniejszy y min (pomiędzy a i b)
                    ab_min_y = min(a[1], b[1])
                    # największy x max (pomiędzy a i b)
                    ab_max_x = max(a[2], b[2])
                    # największy y max (pomiędzy a i b)
                    ab_max_y = max(a[3], b[3])
                    # zastąpienie a i b w compare_list 
                    # na jedną listę zawierającą ekstrema 
                    # z porównania a i b
                    compare_list = [[ab_min_x, ab_min_y, ab_max_x, ab_max_y]]
                    
                # jeśli compare_list zawiera tylko 1 element pomijamy redukcję a i b 
                # (bo jest tylko nowe a) i wracamy do początku pętli w celu dodania 
                # kolejnego b    
        # finalnie compare_list zawiera jedną zwycięzką listę [0]
        # z której wyciągniemy wsp bboxa
        x_min = compare_list[0][0]
        y_min = compare_list[0][1]
        x_max = compare_list[0][2]
        y_max = compare_list[0][3]
        # usługa wfs potrzebuje stringa z odwróconymi wspołrzędnymi
        # oddzielonymi przecinkami
        bbox = f"{y_min},{x_min},{y_max},{x_max}"
        # zapis bboxa do pliku
        with open (f"{self.fname}_bbox.csv", 'w', encoding='utf-8') as f:
            f.write(bbox)
        return bbox

  
    ###########################################
    # CSV + ORGANY.JSON => URL, TYPENAME, EPSG
    ###########################################

    def wfs_param(self):
        # lista do wrzucania wystąpień danego terytu
        licz_teryty = []
        with open(self.csv) as csv:
            for line in csv:
                # pobrabnie 4 pierwszych cyfr terytu
                teryt_powiatu = line.strip()[:4]
                licz_teryty.append(teryt_powiatu)
        # najczęściej występujący teryt
        best_teryt = max(licz_teryty, key=licz_teryty.count)
        # pobranie danych z jsona przygotowanego
        # jednorazowo przez json_exporter.py
        with open ('wfs_param.json') as f:
            d = json.load(f)
        wfs_url = ''
        wfs_typename = ''
        wfs_srsname = ''
        for param in d.values():
            # porównanie z 4 pierwszymi cyframi terytu działki
            if best_teryt == param['teryt'][:4]:
            # wyciągnięcie urla i obcięcie apostrofów
                wfs_url = param['url'][1:-1]
            # wyciagnięcie typename
                wfs_typename = param['typename'][1:-1]
            # wyciągnięcie układu wsp
                wfs_srsname = param['srsname'][1:-1]
        return [wfs_url, wfs_typename, wfs_srsname]


     ###############################################
     #              GML z usługi WFS
     ###############################################   

    def wfs_from_bbox(self):
        # właściwy url dla powiatu
        url = self.wfs_param()[0]
        # nazwa w-wy z działkami w danym powiecie
        typename = self.wfs_param()[1]
        # w jakim układzie powiat wystawia usługę
        srsname = self.wfs_param()[2]
        # pobranie bboxa z ULDK w 1992
        bbox = self.bbox()
        # jeżeli w innym niż 1992
        if srsname != 'EPSG:2180':
            print(f"układ {srsname} !")
            # wyciągnięcie samych xy bboxa bez przecinków
            bboxy= re.findall("\d+\.\d+", self.bbox())
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
            point_min_transform = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{point_min}',2180),2176))")
            point_max_transform = self.postgis.execute(f"SELECT ST_AsText(ST_Transform(ST_GeomFromText('{point_max}',2180),2176))")
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
            query = f"{url}?SERVICE=WFS&REQUEST=GetFeature&version=1.1.0&TYPENAMES={typename}&bbox={bbox}&SRSNAME={srsname}"
            response = requests.get(query)
            if response.status_code == 200:       
                with open(f"{self.fname}.gml", 'wb') as f:
                    f.write(response.content)
                # otwarcie zapisanego przed chwilą gmla
                with open(f"{self.fname}.gml") as f:
                    # gml jest w ukł lokalnym i ma odwórcone osie
                    # osie odwracamy funkcją postgisa ST_Affine
                    # ale nie można do niej wrzucić całego gmla
                    # tylko pojedyncze znaczniki zawierające gml:Polygon
                    soup = BeautifulSoup(f,'xml')
                    # wyciągamy z gmla zawartość tagów <gml:Polygon>
                    gml_Polygons = soup.findAll('gml:Polygon')
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
                        gml_geom = self.postgis.execute(sql)
                        for g in gml_geom:
                            # zapis geometrii lokalnych  
                            # wyciągniętych z GMLA
                            # (trzeba je jeszcze przetransformować do 1992)
                            with open(f"{self.fname}_WFS.csv", 'a') as f:
                                f.write(f"{g[0]}\n")
            else:
                with open(f"{self.fname}_brak_WFS.txt", "w",encoding = 'utf-8') as f:
                        # zapisz w nowej lini teryt którego geometrii serwer nie zwrócił
                            f.write(f" adres {query} zwrócił kod {response.status_code}")
    
        # jeżeli nie trzeba było robić transformacji
        # (dla powiatów z układem 1992)
        # zapis gml bez odwrócenia osi i ponownej transformacji z lokalnego do 1992
        else:
            query = f"{url}?SERVICE=WFS&REQUEST=GetFeature&version=1.1.0&TYPENAMES={typename}&bbox={bbox}&SRSNAME={srsname}"
            response = requests.get(query)
            if response.status_code == 200:
                # zapis gmla - jeśli jest w 1992 to ok
                with open(f"{self.fname}.gml", 'wb') as f:
                    f.write(response.content)
            else:
                with open(f"{self.fname}_brak_WFS.txt", "w",encoding = 'utf-8') as f:
                        # zapisz w nowej lini teryt którego geometrii serwer nie zwrócił
                            f.write(f" adres {query} zwrócił kod {response.status_code}")


pg = ParcelGrabber('./robocze/all.csv')
# pg.geom_from_uldk()
# pg.bbox()
# pg.wfs_from_bbox()