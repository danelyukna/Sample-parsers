import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import HeatMap
import requests
from datetime import datetime
from time import sleep

coords = (55.505659, 37.456671)  # coordinates: latitude, longitude

dist = 20000  # search radius in meters

startDate = datetime(2024, 12, 1)  # first date to search
endDate = datetime(2024, 12, 13) # last date to search

timeperiod = (startDate.timestamp(), endDate.timestamp())

vk_access_token = 'XXX' # api key

vk_vers = 5.131 # current vk version

def getData(coords, timeperiod, offset):
    params = {
        'lat': coords[0],
        'long': coords[1],
        'count': '1000',
        'offset': offset,
        'radius': dist,
        'start_time': timeperiod[0],
        'end_time': timeperiod[1],
        'access_token': vk_access_token,
        'v': vk_vers,
        'sort': 0 # by date of creation
    }
    return requests.get("https://api.vk.com/method/photos.search",
                        params=params, verify=True).json()

def savePoints(resp, df_points):
    try:
        items = resp['response']['items']
    except KeyError:
        return
    for f in items:
        try:
            df_points.loc[len(df_points.index)] = [f['id'], datetime.fromtimestamp(f['date']),f['text'],f['lat'], f['long']]
        except KeyError:
            continue
df_points = pd.DataFrame(columns = ['id', 'date', 'text', 'lat', 'long'])
step = 24*60*60 # step = 1 day
i = timeperiod[0]

while i < timeperiod[1]:
    resp = getData(coords, (i, i+step), 0)
    savePoints(resp, df_points)
    count = resp['response']['count']
    returned = len(resp['response']['items'])
    if count > returned:
        offset = returned
        while offset < count and offset < 3000:
            resp = getData(coords, (i, i+step), offset)
            savePoints(resp, df_points)
            count = resp['response']['count']
            returned = len(resp['response']['items'])
            offset = offset + returned
            if returned == 0:
                break
    i = i + step
    sleep(0.5)

map_data = gpd.GeoDataFrame(df_points, geometry=gpd.points_from_xy(df_points.long, df_points.lat), crs="EPSG:4326")
map_data.to_csv('ryazanovskoe_vk_december2024.csv', index=False,mode='w')