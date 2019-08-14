import os
import shutil
from etl import run

output_path = 'output'
song_input_path = 'input/song-data/*/*/*/*.json'
log_input_path = 'input/log-data/*.json'

if os.path.exists(output_path):
    shutil.rmtree(output_path)

run(song_input_path, log_input_path, output_path)
