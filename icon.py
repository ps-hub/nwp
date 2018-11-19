#!/usr/bin/python3
# coding: utf-8

import os
import sys
import glob
from datetime import datetime, timedelta
import time
import logging
import bz2
import subprocess

import requests

class ICON:
    def __init__(self, root_dir, cdo_target, cdo_weight):
        self.VAR = ['T_2M', 'TD_2M']
        self.FFF = ['006', '012', '018']

        self.cdo_target = cdo_target
        self.cdo_weight = cdo_weight

        self.root_dir = root_dir
        self.grib_dir = os.path.join(self.root_dir, 'grib')
        self.tif_dir = os.path.join(self.root_dir, 'tif')
        self.txt_dir = os.path.join(self.root_dir, 'txt')
        for directory in [self.grib_dir, self.tif_dir, self.txt_dir]:
            if not os.path.exists(directory):
                os.mkdir(directory)

        # инициализация протоколирования
        log_file = os.path.join(self.root_dir, 'icon.log')
        logging.basicConfig(handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
                            level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%H:%M:%S %d.%m.%Y')

    def download(self, dtime):
        root_url = 'https://opendata.dwd.de/weather/nwp/icon/grib'
        for fff in self.FFF:
            for var in self.VAR:
                model_run = dtime[8:]
                file_name = f'icon_global_icosahedral_single-level_{dtime}_{fff}_{var}.grib2.bz2'
                url = f'{root_url}/{model_run}/{var.lower()}/{file_name}'
                
                response = requests.get(url)
                if response.status_code == 200:
                    out_dir = os.path.join(self.grib_dir, dtime)
                    out_file = os.path.join(out_dir, file_name)
                    if not os.path.exists(out_dir):
                        os.mkdir(out_dir)
                    with open(out_file, 'wb') as f:
                        f.write(response.content)
                    logging.info(f'Download - {dtime}/{file_name}')
                    time.sleep(1)
                else:
                    logging.error(f'Download - {response.url}')
                    time.sleep(1)

    def unpack(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        for inp_file in glob.glob(f'{inp_dir}//*.bz2'):
            out_file = os.path.splitext(inp_file)[0]
            out_name = os.path.basename(out_file)
            with open(out_file, 'wb') as f, bz2.BZ2File(inp_file) as bz:
                for data in iter(lambda : bz.read(100 * 1024), b''):
                    f.write(data)
            logging.info(f' Unpack  - {dtime}/{out_name}')

    def cdo_remap(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        for inp_file in glob.glob(f'{inp_dir}//*icosahedral*.grib2'):
            out_file = inp_file.replace('icosahedral', 'latlon')
            out_name = os.path.basename(out_file)
            cmd = f'cdo -f grb2 remap,{self.cdo_target},{self.cdo_weight} {inp_file} {out_file}'
            subprocess.call(cmd, shell=True)
            logging.info(f'CDO Remap - {dtime}/{out_name}')

    def translate(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        if os.path.exists(inp_dir):
            out_dir = os.path.join(self.tif_dir, dtime)
            if not os.path.exists(out_dir):
                os.mkdir(out_dir)
            for inp_file in glob.glob(f'{inp_dir}//*latlon*.grib2'):
                out_name = os.path.basename(inp_file).replace('.grib2','.tif')
                out_file = os.path.join(out_dir, out_name)
                cmd = f'gdal_translate -a_srs EPSG:4326 -ot Float32 {inp_file} {out_file}'
                subprocess.call(cmd, shell=True)
                logging.info(f'Translate - {dtime}/{out_name}')
        else:
            logging.error(f'Translate - Not Found Input Directory - {inp_dir}')

if __name__ == '__main__':
    root_dir = r'/cygdrive/c/Users/user/Desktop/icon/data'
    cdo_target = os.path.join(root_dir, 'cdo/target_grid_world_0125_xfirst_0.txt')
    cdo_weight = os.path.join(root_dir, 'cdo/weights_icogl2world_0125_xfirst_0.nc')
    dtime = '2018111906'
    
    icon = ICON(root_dir, cdo_target, cdo_weight)
    #icon.download(dtime)
    #icon.unpack(dtime)
    icon.cdo_remap(dtime)
    icon.translate(dtime)