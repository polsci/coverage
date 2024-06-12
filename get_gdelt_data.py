import requests
import json
import pandas as pd
import time
import datetime

def get_gdelt_data_for_country(sourcecountry, days_ago = 1):
    startdatetime = (datetime.datetime.utcnow() - datetime.timedelta(days=days_ago)).strftime('%Y%m%d000000')
    finaldatetime = (datetime.datetime.strptime(startdatetime, '%Y%m%d%H%M%S') + datetime.timedelta(days=1)).strftime('%Y%m%d000000')

    maxrecords = 250
    extraquery = ''
    df = None
    has_data = True
    enddatetime = finaldatetime
    while has_data == True:
        url = f'https://api.gdeltproject.org/api/v2/doc/doc?query=%20sourcecountry:{sourcecountry}{extraquery}&mode=ArtList&maxrecords={maxrecords}&format=json&startdatetime={startdatetime}&enddatetime={enddatetime}'
        try:
            result = requests.get(url, timeout=5)
        except requests.exceptions.RequestException as e:
            print(e)
            # retry after 10 seconds
            print('retrying in 10 seconds')
            time.sleep(10)
            try: 
                result = requests.get(url, timeout=5)
            except requests.exceptions.RequestException as e:
                print(e)
                print('Retry failed. Exiting.')
                break

        try:
            data = result.json()    
        except json.decoder.JSONDecodeError as e:
            if (result.text.strip() == 'Timespan is too short.'):
                data = {}
            else:
                print(result.content)
                raise

        if len(data) > 0:
            print(startdatetime,enddatetime,len(data['articles']))
            if len(data['articles']) == maxrecords:
                print('Result appears to be larger than 250 records, trying 1 hour increments ...')
            else:
                if df is None:
                    df = pd.DataFrame(data['articles'])
                else:
                    df = pd.concat([df, pd.DataFrame(data['articles'])])
                # shift startdatetime and enddatetime by 1 hour
                startdatetime = enddatetime
        else:
            print(startdatetime,enddatetime,0)
            startdatetime = enddatetime
        
        enddatetime = (datetime.datetime.strptime(startdatetime, '%Y%m%d%H%M%S') + datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')

        if finaldatetime <= startdatetime:
            has_data = False
        else:
            time.sleep(2) # respectful pause

    if df is not None:
        df = df.drop_duplicates()
        print('Total Rows:', df.shape[0])

    return df
