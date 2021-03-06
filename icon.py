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

from osgeo import gdal, ogr
import numpy as np
import requests
import schedule


class ICON:
    def __init__(self, root_dir, cdo_target, cdo_weight):
        # self.VAR = ['t_2m']
        # self.FFF = ['003']
        self.VAR = ['t_2m', 'td_2m', 'tot_prec', 'u_10m', 'v_10m']
        self.FFF = ['003', '006', '009', '012', '015', '018', '021', '024', '027']
        self.EXTENT = {'leftlon': 35, 'rightlon': 65, 
                       'toplat': 65, 'bottomlat': 50}
        
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
                file_name = f'icon_global_icosahedral_single-level_{dtime}_{fff}_{var.upper()}.grib2.bz2'
                url = f'{root_url}/{model_run}/{var}/{file_name}'
                
                response = requests.get(url)
                if response.status_code == 200:
                    out_dir = os.path.join(self.grib_dir, dtime)
                    out_name = f'icon.{dtime}.{fff}.{var}.grib2.bz2'
                    out_file = os.path.join(out_dir, out_name)
                    if not os.path.exists(out_dir):
                        os.mkdir(out_dir)
                    with open(out_file, 'wb') as f:
                        f.write(response.content)
                    logging.info(f'Download - {dtime}/{out_name}')
                    time.sleep(1)
                else:
                    logging.error(f'Download - {response.url}')
                    time.sleep(1)

    def unpack(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        for inp_file in glob.glob(f'{inp_dir}//*.bz2'):
            out_name = os.path.basename(inp_file).replace('icon', 'icon_ico').replace('.bz2', '')
            out_file = os.path.join(self.grib_dir, dtime, out_name)
            with open(out_file, 'wb') as f, bz2.BZ2File(inp_file) as bz:
                for data in iter(lambda : bz.read(100 * 1024), b''):
                    f.write(data)
            logging.info(f' Unpack  - {dtime}/{out_name}')

    def cdo_remap(self, dtime):
        inp_dir = os.path.join(self.grib_dir, dtime)
        for inp_file in glob.glob(f'{inp_dir}//*ico*.grib2'):
            out_file = inp_file.replace('icon_ico', 'icon_reg')
            out_name = os.path.basename(out_file)
            cmd = f'cdo -f grb2 remap,{self.cdo_target},{self.cdo_weight} {inp_file} {out_file}'
            subprocess.call(cmd, shell=True)
            logging.info(f'CDO Remap - {dtime}/{out_name}')
            os.remove(inp_file)

    def translate(self, dtime):

        def calc_wind(fff, out_dir):
            inp_name_u = f'icon.{dtime}.{fff}.u_10m.tif'
            inp_name_v = f'icon.{dtime}.{fff}.v_10m.tif'
            inp_file_u = os.path.join(out_dir, inp_name_u)
            inp_file_v = os.path.join(out_dir, inp_name_v)

            if os.path.exists(inp_file_u) and os.path.exists(inp_file_v):
                raster_u = gdal.Open(inp_file_u)
                raster_v = gdal.Open(inp_file_v)
                array_u = raster_u.ReadAsArray().astype(np.float32)
                array_v = raster_v.ReadAsArray().astype(np.float32)

                out_name = f'icon.{dtime}.{fff}.ws_10m.tif'
                out_file = os.path.join(out_dir, out_name)
                row, col = array_u.shape
                band = 1
                dtype = gdal.GDT_Float32
                driver = gdal.GetDriverByName("GTiff")
                out_raster = driver.Create(out_file, col, row, band, dtype)

                proj = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'
                transform = raster_u.GetGeoTransform()
                out_raster.SetProjection(proj)
                out_raster.SetGeoTransform(transform)

                out_array = (array_u**2 + array_v**2)**0.5
                out_raster.GetRasterBand(1).WriteArray(out_array)
                
                logging.info(f'Translate - {dtime}/{out_name}')
                os.remove(inp_file_u)
                os.remove(inp_file_v)
        
        inp_dir = os.path.join(self.grib_dir, dtime)
        if os.path.exists(inp_dir):
            out_dir = os.path.join(self.tif_dir, dtime)
            if not os.path.exists(out_dir):
                os.mkdir(out_dir)
            for fff in self.FFF:
                for var in self.VAR:
                    inp_name = f'icon_reg.{dtime}.{fff}.{var}.grib2'
                    inp_file = os.path.join(inp_dir, inp_name)
                    out_name = f'icon.{dtime}.{fff}.{var}.tif'
                    out_file = os.path.join(out_dir, out_name)

                    projWin = [self.EXTENT['leftlon'],   # ulx
                               self.EXTENT['toplat'],    # uly
                               self.EXTENT['rightlon'],  # lrx
                               self.EXTENT['bottomlat']] # lry
                    gdal.Translate(out_file,
                                inp_file,
                                outputType=gdal.GDT_Float32,
                                outputSRS='EPSG:4326',
                                projWin=projWin)
                    logging.info(f'Translate - {dtime}/{out_name}')
                    os.remove(inp_file)
                
                calc_wind(fff, out_dir) # дополнительно расчет скорости ветра
        else:
            logging.error(f'Translate - Not Found Input Directory - {inp_dir}')

    def process(self, dtime):
        self.download(dtime)
        self.unpack(dtime)
        self.cdo_remap(dtime)
        self.translate(dtime) 

    def daemon(self):
        logging.info(f'Daemon - Start')

        def activate(delta):
            dtime = (datetime.now() - timedelta(hours=delta)).strftime("%Y%m%d%H")
            self.process(dtime)

        # local time
        schedule.every().day.at('06:55').do(activate, 6) # cycle 00

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    root_dir = r'/cygdrive/c/Users/user/Desktop/icon/data'
    cdo_target = os.path.join(root_dir, 'cdo/target_grid_world_0125_xfirst_0.txt')
    cdo_weight = os.path.join(root_dir, 'cdo/weights_icogl2world_0125_xfirst_0.nc')
    dtime = '2018112000'
    
    icon = ICON(root_dir, cdo_target, cdo_weight)
    #icon.download(dtime)
    #icon.unpack(dtime)
    #icon.cdo_remap(dtime)
    #icon.translate(dtime)
    #icon.process(dtime)