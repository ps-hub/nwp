# -*- coding: utf-8 -*-

import os
import glob
import requests
import bz2

class ICON:
    ROOT_URL = 'https://opendata.dwd.de/weather/nwp/icon/grib'
    def __init__(self):
        self.FFF = ['006'] #['006', '012', '018']
        self.VAR = ['T_2M']#, 'TD_2M']
        self.grib_dir = r'C:\Users\user\Desktop\icon\data'

    def download(self, dtime):
        for fff in self.FFF:
            for var in self.VAR:
                model_run = dtime[8:]
                file_name = f'icon_global_icosahedral_single-level_{dtime}_{fff}_{var}.grib2.bz2'
                url = f'{self.ROOT_URL}/{model_run}/{var.lower()}/{file_name}'
                
                response = requests.get(url)
                if response.status_code == 200:
                    out_dir = os.path.join(self.grib_dir, dtime)
                    out_file = os.path.join(out_dir, file_name)
                    if not os.path.exists(out_dir):
                        os.mkdir(out_dir)
                    with open(out_file, 'wb') as f:
                        f.write(response.content)
                    print(file_name, '-- download')
                else:
                    print(response.url, '-- error')

    def decompress(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        for inp_file in glob.glob(f'{inp_dir}//**/*.bz2', recursive=True): # TODO хз про рекурсивность
            out_file = os.path.splitext(inp_file)[0]
            with open(out_file, 'wb') as f, bz2.BZ2File(inp_file) as bz:
                for data in iter(lambda : bz.read(100 * 1024), b''):
                    f.write(data)


if __name__ == '__main__':
    icon = ICON()
    icon.decompress('2018111700')