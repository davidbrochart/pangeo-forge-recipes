import os
import sys
import logging
import tempfile
from datetime import datetime, timedelta

import aiohttp
from fsspec.implementations.local import LocalFileSystem

from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
from pangeo_forge.storage import CacheFSSpecTarget, FSSpecTarget

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

t0 = datetime(2000, 6, 1)
t1 = datetime(2021, 1, 1)
dt = timedelta(minutes=30)

class Dates:
    def __init__(self, t0, t1, dt):
        self.t0 = t0
        self.t1 = t1
        self.dt = dt

    def __iter__(self):
        self.t = self.t0
        return self

    def __next__(self):
        if self.t < self.t1:
            t = self.t
            self.t += self.dt
            return t
        else:
            raise StopIteration


dates = Dates(t0, t1, dt)

input_urls = [f'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/{t.year}{t.month:02}/3B-HHR-L.MS.MRG.3IMERG.{t.year}{t.month:02}{t.day:02}-S{t.hour:02}{t.minute:02}00-E{t.hour:02}{t.minute+29}59.{t.hour*60+t.minute:04}.V06B.RT-H5' for t in iter(dates)]

def add_time(ds, name):
    fname = name[name.rfind('/') + 1:]
    year = int(fname[23:27])
    month = int(fname[27:29])
    day = int(fname[29:31])
    hour = int(fname[33:35])
    minute = int(fname[35:37]) + 15
    t = datetime(year, month, day, hour, minute)
    ds = ds.assign_coords(time=[t])
    return ds


recipe = NetCDFtoZarrSequentialRecipe(
    input_urls=input_urls,
    sequence_dim="time",
    inputs_per_chunk=4,
    xarray_open_kwargs={'group': 'Grid', 'drop_variables': ['time_bnds', 'lon_bnds', 'lat_bnds']},
    fsspec_open_kwargs={'client_kwargs': {'auth': aiohttp.BasicAuth(os.environ['GPM_IMERG_USERNAME'], os.environ['GPM_IMERG_PASSWORD'])}},
    process_input=add_time,
)

fs_local = LocalFileSystem()

cache_dir = tempfile.TemporaryDirectory()
cache_target = CacheFSSpecTarget(fs_local, cache_dir.name)

target_dir = tempfile.TemporaryDirectory()
target = FSSpecTarget(fs_local, target_dir.name)

recipe.input_cache = cache_target
recipe.target = target

all_chunks = list(recipe.iter_chunks())

for input_file in recipe.inputs_for_chunk(all_chunks[0]):
    recipe.cache_input(input_file)

recipe.prepare_target()

recipe.store_chunk(all_chunks[0])
