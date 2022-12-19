import json, csv

# dane z powiatów do przerobienia na prawidłowego jsona

sources = {
    "Prezydent Miasta Bielska-Białej":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg.bielsko-biala.pl/bielsko-egib' version='auto'",
    "Prezydent Miasta Gdańska":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://ewid-wms.gdansk.gda.pl/iip/ows' version='auto'",
    "Prezydent Miasta Gorzowa Wielkopolskiego":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ewns:dzialki' url='https://geoportal.wms.um.gorzow.pl/map/geoportal/wfs.php' version='auto'",
    "Prezydent Miasta Krakowa":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='EGIB_udostepnianie:dzialki' url='https://msip.um.krakow.pl/arcgis/services/ZSOZ/EGIB_udostepnianie/MapServer/WFSServer' version='auto'",
    "Prezydent Miasta Poznania":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://portal.geopoz.poznan.pl/wmsegib' version='auto'",
    "Prezydent Miasta Rzeszowa":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://osrodek.erzeszow.pl/map/geoportal/wfs.php' version='auto'",
    "Prezydent Miasta Stołecznego Warszawy":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='wfs:dzialki_free' url='https://wms2.um.warszawa.pl/geoserver/wfs/wms' version='auto'",
    "Prezydent Miasta Szczecin":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://wms.e-osrodek.szczecin.pl/szczecin-egib' version='auto'",
    "Prezydent Miasta Tychy":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='default:Dzialki' url='http://sit.umtychy.pl/isdp/gs/ows/default/wfs4' version='auto'",
    "Prezydent Miasta Włocławek":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ewns:dzialki' url='https://geoportal.wloclawek.eu/map/geoportal/wfs.php' version='auto'",
    "Prezydent Miasta Świętochłowice":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ewns:dzialki' url='https://swietochlowice.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Bieruńsko-Lędziński":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='egib:EGB_DzialkaEwidencyjna' url='https://sbl.webewid.pl:8443/us/wfs/sip' version='auto'",
    "Starosta Bytowski":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://bytowski.webewid.pl:4433/iip/ows' version='auto'",
    "Starosta Będziński":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg.powiat.bedzin.pl/bedzin-egib' version='auto'",
    "Starosta Gryficki":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg.podgikgryfice.pl/gryfice-egib' version='auto'",
    "Starosta Jaworski":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ewns:dzialki' url='https://jawor.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Jędrzejowski":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://jedrzejow.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Kamieński":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg.powiatkamienski.pl/kamien' version='auto'",
    "Starosta Koszaliński":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='dzialki' url='https://koszalinski-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Kołobrzeski":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ms:dzialki' url='https://kolobrzeski-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Legnicki":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ms:dzialki' url='https://legnicki-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Limanowski":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='dzialki' url='https://limanowski-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Nowotarski":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://nowotarski.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Nowotomyski":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://wms.powiatnowotomyski.pl/nowytomysl-egib' version='auto'",
    "Starosta Piaseczyński":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://wms.epodgik.pl/cgi-bin/piaseczno' version='auto'",
    "Starosta Powiatu Gdańskiego":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://gdanski-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Powiatu Gryfińskiego":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ms:dzialki' url='https://gryfinski.webewid.pl:4439/iip/ows' version='auto'",
    "Starosta Powiatu Hrubieszowskiego":"restrictToRequestBBOX='1' srsname='EPSG:2179' typename='ewns:dzialki' url='https://hrubieszow.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Powiatu Kartuskiego":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://kartuski-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Powiatu Kieleckiego":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://geoportal.powiat.kielce.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Powiatu Krakowskiego":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ms:dzialki' url='https://wms.powiat.krakow.pl:1518/iip/ows' version='auto'",
    "Starosta Powiatu Mieleckiego":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://mielec.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Powiatu Radomskiego":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://radom.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Powiatu Szczecineckiego":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='dzialki' url='https://szczecinecki-wms.webewid.pl/iip/ows' version='auto'",
    "Starosta Powiatu Żarskiego":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ewns:dzialki' url='https://zary.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Poznański":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg.podgik.poznan.pl/wms-poznanski' version='auto'",
    "STAROSTA PRZYSUSKI":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://przysucha.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Sokólski":"restrictToRequestBBOX='1' srsname='EPSG:2179' typename='ewns:dzialki' url='https://powiatsokolski.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Stargardzki":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://ikerg2.powiatstargardzki.eu/stargard-egib' version='auto'",
    "Starosta Sulęciński":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ewns:dzialki' url='https://sulecin.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Suski":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://powiatsuski.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Szydłowiecki":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ewns:dzialki' url='https://szydlowiecpowiat.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Tarnowski":"restrictToRequestBBOX='1' srsname='EPSG:2178' typename='ms:dzialki' url='https://webewid.powiat.tarnow.pl:20443/iip/ows' version='auto'",
    "Starosta Tczewski":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ms:dzialki' url='https://wms.powiat.tczew.pl/iip/ows' version='auto'",
    "Starosta Wejherowski":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://wms.epodgik.pl/cgi-bin/wejherowo' version='auto'",
    "Starosta Włocławski":"restrictToRequestBBOX='1' srsname='EPSG:2177' typename='ewns:dzialki' url='https://wloclawek.geoportal2.pl/map/geoportal/wfs.php' version='auto'",
    "Starosta Zgorzelecki":"restrictToRequestBBOX='1' srsname='EPSG:2180' typename='ms:dzialki' url='https://iegib.powiat.zgorzelec.pl/zgorzelec-egib' version='auto'",
    "Starosta Żagański":"restrictToRequestBBOX='1' srsname='EPSG:2176' typename='ms:dzialki' url='https://zaganski-wms.webewid.pl/iip/ows' version='auto'"
}

# tymczasowy słownik
dane = {}
# przerobienie sources na poprawnego jsona
for organ, params in sources.items():
    params_list=params.split(' ')
    d ={}
    for param in params_list:
        p_list=param.split('=')
        d[p_list[0]] = p_list[1]
    dane[organ] = d


# dorzucenie do przygotowanego
# wcześniej słownika terytu z csv
for org, param in dane.items():
    with open ('organ_teryt.csv') as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
                if org == row[0]:
                    print(org,'==>',row[1])
                    dane[org]['teryt']=row[1]

    
# export do json, który będzie zawierał
# wszystkie potrzebne parametry do zapytania WFS       
with open('wfs_param.json','w', encoding='utf-8') as f:
    json.dump(dane, f, indent=1)





