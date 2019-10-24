import os

import pandas as pd
import requests

home = os.environ['HOME']

def nasa_giss():
    print('NASA Goddard Institute for Space Studies')
    print('https://data.giss.nasa.gov/gistemp/')

    glbl = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'
    north = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv'
    south = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv'

    res = requests.get(south)
    os.makedirs('./data', exist_ok=True)
    with open('./data/south-temps.csv', 'wb') as fi:
        fi.write(res.content)

    data = pd.read_csv('./data/south-temps.csv', header=1, index_col=0)
    data = data.replace("***", 0)
    data = data.iloc[:-1, :]
    data = data.astype(float)


def get_csv(name):
    print('getting {}'.format(name))
    with open(raw / name, 'wb') as fp:
        ftp.retrbinary('RETR '+name, fp.write)

    with open(raw / name, 'r') as fi:
        for num, line in enumerate(fi):
            if line[0] != '#':
                break

    home = './'
    data = pd.read_csv(
        (raw / name).resolve(),
        skiprows=num
    )
    data.to_csv(clean / name)

if __name__ == '__main__':


    from pathlib import Path
    home = Path.home() / 'climate-data'
    home.mkdir(exist_ok=True, parents=True)

    clean = home /  'clean'
    clean.mkdir(exist_ok=True, parents=True)

    raw = home / 'raw'
    raw.mkdir(exist_ok=True, parents=True)

    from ftplib import FTP
    ftp = FTP('aftp.cmdl.noaa.gov')
    ftp.login()
    ftp.cwd('products')
    ftp.cwd('trends')
    ftp.cwd('co2')
    print(ftp.retrlines('LIST'))
    csvs = [fi for fi in ftp.nlst('.') if '.csv' in fi]

    # name = 'co2_gr_mlo.csv'
    for csv in csvs:
        get_csv(csv)
