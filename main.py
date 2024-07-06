from co_embedding_indexer import run_azure_search_indexing, run_query_search
from video_streamer import run_download_and_upload
from config_manager import load_config


def main_livestream_indexing():
    # load configuration
    config = load_config()

    # Azure Video Indexer parameters
    stream = "https://www.youtube.com/watch?v=bNyUyrR0PHo"
    video_duration = "00:00:05"
    output_folder = config['main']['workingDir']
    num_iter = 10 ** 7
    config['vi']['language'] = 'Arabic'
    run_download_and_upload(stream, video_duration, output_folder, num_iter, config)
    print("done")


def main_semantic_search():
    # load configuration
    config = load_config()
    run_azure_search_indexing(config)
    run_query_search(config, "Red car")
    print("done")


if __name__ == '__main__':
    main_semantic_search()
