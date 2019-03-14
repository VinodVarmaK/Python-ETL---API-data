import datetime
import json
import urllib.request

 def url_builder(city_id):
    user_api = '32bd71a6bae5381b136e7667c5315edb'  # Obtained after registering with Open weather form: http://openweathermap.org/
    unit = 'metric'  
    api = 'http://api.openweathermap.org/data/2.5/weather?id='     # Berlin city id = 2950159 in http://openweathermap.org/
    full_api_url = api + str(2950159) + '&mode=json&units=' + unit + '&APPID=' + user_api
    return full_api_url

#data pre processing 
def time_converter(time):
    converted_time = datetime.datetime.fromtimestamp(
        int(time)
    ).strftime('%I:%M %p')
    return converted_time


def data_fetch(full_api_url):
    url = urllib.request.urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output)
    url.close()
    return raw_api_dict
    
def data_organizer(raw_api_dict):
    data = dict(
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        humidity=raw_api_dict.get('main').get('humidity'),
        pressure=raw_api_dict.get('main').get('pressure'),
        sky=raw_api_dict['weather'][0]['main'],
        sunrise=time_converter(raw_api_dict.get('sys').get('sunrise')),
        sunset=time_converter(raw_api_dict.get('sys').get('sunset')),
        wind=raw_api_dict.get('wind').get('speed'),
        wind_deg=raw_api_dict.get('deg'),
        dt=time_converter(raw_api_dict.get('dt')),
        cloudiness=raw_api_dict.get('clouds').get('all')
    )
    return data
    
    
def data_output(data):
    m_symbol = '\xb0' + 'C'
    print('Current weather in: {}, {}:'.format(data['city'], data['country']))
    print('temparature: {}'.format(data['temp']), m_symbol )
    print('Climate: {}'.format(data['sky']))
    print('Max: {}'.format(data['temp_max']))
    print('Min: {}'.format(data['temp_min']))
    print('Wind Speed: {}'.format(data['wind']))
    print('Humidity: {}'.format(data['humidity']))
    print('Cloud: {}'.format(data['cloudiness']))
    print('Pressure: {}'.format(data['pressure']))
    print('Sunrise at: {}'.format(data['sunrise']))
    print('Sunset at: {}'.format(data['sunset']))
    print('Last update from the server: {}'.format(data['dt']))
    
    
 if __name__ == '__main__':
    try:
     data = data_output(data_organizer(data_fetch(url_builder(2172797))))
    except IOError:
        print('no internet')
        

#Writing the data to CSV file in local system
my_dict=data
with open('mycsvfile.csv', 'w') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, my_dict.keys())
    w.writeheader()
    w.writerow(my_dict)
    
#connecting to Mysql database     
import csv
import mysql.connector    
cnx = mysql.connector.connect(user='root', password='root',
                              host='localhost', port="3306",  database='test')

cursor = mydb.cursor()

#reading the data from downloaded CSV file and loading it into a test fact table
csv_data = csv.reader(open('C:/Users/Vinod Varma/Documents/mycsvfile.csv'))
for row in csv_data:

    cursor.execute('INSERT INTO test_weather(sunrise,sunset,wind,sky,temp_min,temp_max,wind_deg,pressure,temp, \
                   humidity,dt,cloudiness,country,city)' \
          'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
          row)
#close the connection to the database.
mydb.commit()
cursor.close()
print ("Done")

