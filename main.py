import json
import os

from video_streamer import run_download_and_upload


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '', 'config.json')
    if os.path.isfile(config_path):
        with open(config_path) as data_file:
            data = json.load(data_file)
            return data


def main():
    # load configuration
    config = load_config()

    # Azure Video Indexer parameters
    STREAMURL = "https://www.youtube.com/watch?v=bNyUyrR0PHo"
    VIDEO_DURATION = "00:00:05"
    output_folder = config['main']['workingDir']
    NUM_ITER = 10 ** 7
    config['vi']['language'] = 'Arabic'
    run_download_and_upload(STREAMURL, VIDEO_DURATION, output_folder, NUM_ITER, config)
    print("done")


if __name__ == '__main__':
    main()
