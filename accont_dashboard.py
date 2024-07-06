import json
import os

from tqdm import tqdm
from azure_data_explorer import AzureDataExplorerClient
from main import load_config
from video_indexer_wrapper import VideoIndexerWrapper


class AccountDashboardUploader:
    def __init__(self, config):
        self.video_indexer = VideoIndexerWrapper(**config['vi'])
        self.adx_wrapper = AzureDataExplorerClient(config['adx']['cluster_url'], config['adx']['database_name'])
        self.table_name = config['adx']['table_name']
        self.mapping_name = config['adx']['mapping_name']

    def _get_all_vi_index_jsons(self, insights_repo_path):
        """Returns a list of all the video index JSONs for the given video."""
        indexed_videos = self.video_indexer.list_all_indexed_videos()
        print(f'Start downloading video insights for {len(indexed_videos)} videos...')
        for video_id in tqdm(indexed_videos):
            video_insights = self.video_indexer.get_video_index(video_id['id'])
            with open(f'{insights_repo_path}/{video_id["id"]}.json', 'w') as f:
                json.dump(video_insights, f)
        return

    def _upload_jsons_to_adx(self, jsons_repo: str):
        for json_file_name in os.listdir(jsons_repo):
            json_file = os.path.join(jsons_repo, json_file_name)
            # with open(json_file) as f:
            #     data = json.load(f)
            #     self.adx_wrapper.ingest_data_from_json(data, self.table_name, self.mapping_name)
            try:
                self.adx_wrapper.ingest_data_from_json(json_file, self.table_name, self.mapping_name)
            except Exception as e:
                print(f"Error during ingestion: {e}")
                continue

    def ingest_videos(self, video_path: str):
        # self._get_all_vi_index_jsons(video_path)
        self._upload_jsons_to_adx(video_path)


def main_ingestion():
    config = load_config()
    account_dashboard_uploader = AccountDashboardUploader(config)
    account_dashboard_uploader.ingest_videos(config['main']['workingDir'])


if __name__ == '__main__':
    main_ingestion()
