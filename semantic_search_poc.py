import os
import time, json, zipfile

from video_indexer_wrapper import VideoIndexerWrapper
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.search.documents import SearchClient
from azure.identity import DefaultAzureCredential
from config_manager import load_config


class SemanticSearchIndexer:
    def __init__(self, config):
        self.video_indexer_wrapper = VideoIndexerWrapper(**config['vi'])
        credential = DefaultAzureCredential()
        ais_params = config['ais']
        ais_params['credential'] = credential
        self.search_client = SearchClient(**ais_params)
        self.account_url = config['storage']['sa_url']
        self.blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)

    def index_media_file(self, media_asset_url: str, working_dir: str):
        """
        Index a media asset file in Azure AI Video Indexer, download its keyframes and Prompt Content into a blob.
        :param working_dir: local directory to store the extracted data
        :param media_asset_url: the media asset URL
        :return: None
        """
        # Upload a video to Video Indexer
        video_response = self.video_indexer_wrapper.upload_video(media_asset_url)
        if 'ErrorType' in video_response:
            message = video_response['Message']
            video_id = message[52:62]
        elif video_response is None or 'id' not in video_response:
            print(f'Failed to upload the video.')
            raise Exception(f'Failed to upload the video. Message: {video_response}')
        else:
            video_id = video_response['id']

        # Get the indexed
        indexed_video = self.video_indexer_wrapper.get_video_index(video_id)
        if indexed_video['state'] == 'Processing':
            print(f'Video is not processed yet. Starting polling...')
            while indexed_video['state'] == 'Processing':
                time.sleep(15)
                indexed_video = self.video_indexer_wrapper.get_video_index(video_id)
                print(f'Video state: {indexed_video["state"]}')

        # create a prompt content
        prompt_content = try_get_prompt(self.video_indexer_wrapper, video_id)

        # Extract prompt content and keyframes
        prompt_sections = prompt_content['sections']

        # Extract keyframes - get the artifacts
        keyframes = extract_keyframes(self.video_indexer_wrapper, video_id, working_dir)

        # Upload the extracted keyframes
        for keyframe in keyframes:
            keyframe_name = os.path.basename(keyframe)
            blob_client = self.blob_service_client.get_blob_client(video_id, blob=keyframe_name)
            # Create a new blob and upload the extracted data
            # blob_client = self.blob_service_client.get_blob_client(container=video_id, blob=keyframe)
            with open(keyframe, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)

        # Upload the extracted prompt content sections
        for prompt_section in prompt_sections:
            # create a JSON file for each prompt section
            json_file_name = f'{video_id}_{prompt_section["id"]}.json'
            json_section_path = f'{working_dir}/{json_file_name}'
            with open(json_section_path, 'w') as f:
                prompt_section_metadata = dict(video_id=video_id, segment_id=prompt_section['id'],
                                               video_name=prompt_content['name'], content=prompt_section['content'],
                                               start_time=prompt_section['start'], end_time=prompt_section['end'])
                json.dump(prompt_section_metadata, f)
            # Create a new blob and upload the extracted data
            blob_client = self.blob_service_client.get_blob_client(video_id, blob=json_file_name)
            with open(json_section_path, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)

        print('Data uploaded successfully!')


def try_get_prompt(vi_client, video_id):
    """
    Get the prompt content of the video if exists. Otherwise, create it and return the prompt content.
    :param vi_client:
    :param video_id: the video id
    :return: prompt content dict.
    """
    prompt_content = None
    prompt_content_response = vi_client.get_prompt_content(video_id)
    if prompt_content_response is None or prompt_content_response.status != 200:
        print(f'Failed to get prompt content for video: {video_id}')
        vi_client.create_prompt_content(video_id)
        prompt_content_response = vi_client.get_prompt_content(video_id)
        while prompt_content_response is not None and prompt_content_response.status != 200:
            time.sleep(15)
            prompt_content = json.loads(prompt_content_response.read())
    if prompt_content is None:
        prompt_content = json.loads(prompt_content_response.read())
    return prompt_content


def extract_keyframes(vi_client, video_id, working_dir):
    kf_video_id_to_zip, failed_video_ids = vi_client.download_keyframes([dict(id=video_id)], working_dir)

    # unzip the file into a temp directory
    temp_directory = kf_video_id_to_zip[video_id][:-4]

    # os.system(f'unzip {temp_directory}.zip -d {temp_directory}')
    # Create the extract directory if it doesn't exist
    os.makedirs(temp_directory, exist_ok=True)

    # Open the zip file
    with zipfile.ZipFile(kf_video_id_to_zip[video_id], 'r') as zip_ref:
        # Extract all the contents to the specified directory
        zip_ref.extractall(temp_directory)
    # upload images to Azure AI Search
    return [os.path.join(temp_directory, fp) for fp in os.listdir(temp_directory) if fp.endswith('.jpg')]


def main():
    config = load_config()
    example_video = config['main']['demo_video_path']
    semantic_search_indexer = SemanticSearchIndexer(config).index_media_file(example_video, config['main']['workingDir'])
    working_dir = config['main']['workingDir']

    # index a video
    semantic_search_indexer.index_media_file(example_video, working_dir)

    # Initialize Search client
    search_client = SearchClient('YOUR_SERVICE_NAME', 'YOUR_INDEX_NAME', 'YOUR_ADMIN_KEY')

    # Create a data source
    data_source = search_client.create_data_source('your_data_source', 'azureblob', 'YOUR_CONNECTION_STRING', 'your_container')

    # Create a skillset with CLIP as a skill
    skillset = search_client.create_skillset('your_skillset', 'clip', 'description', 'context', 'inputs', 'outputs')

    # Create an index that uses the skillset
    index = search_client.create_index('your_index', 'fields', 'scoringProfiles', 'defaultScoringProfile', 'corsOptions', skillset)

    # Create an indexer that connects the data source and the index
    indexer = search_client.create_indexer('your_indexer', data_source, index)

    # Run the indexer
    search_client.run_indexer(indexer)


if __name__ == '__main__':
    main()
    print('Done!')
