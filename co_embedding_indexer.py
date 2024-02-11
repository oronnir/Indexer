"""
Assuming the account has indexed videos, generate PromptContent and download keyframes to index with CLIP.
"""
import json
import os

from main import load_config
from azure_ai_search_wrapper import AzureAISearchWrapper
from video_indexer_wrapper import VideoIndexerWrapper


class CoEmbeddingsIndexer:
    def __init__(self, config):
        self.video_indexer_wrapper = VideoIndexerWrapper(**config['vi'])
        self.azure_ai_search_wrapper = AzureAISearchWrapper()

    def upload_texts_to_azure_ai_search(self, prompt_content_json_path, video_id):
        prompt_content_json = json.load(open(prompt_content_json_path))
        for prompt_segment in prompt_content_json['sections']:
            segment_id = prompt_segment['id']
            start_time = prompt_segment['start']
            end_time = prompt_segment['end']
            prompt = prompt_segment['content']
            item_index = dict(video_id=video_id, segment_id=segment_id, start_time=start_time, end_time=end_time)
            self.azure_ai_search_wrapper.upload_textual_content(prompt, item_index)

    def index_image_zip(self, file, video_id, working_directory):
        # unzip the file into a temp directory
        temp_directory = f'{working_directory}/{file}'[:-4]
        os.system(f'unzip {temp_directory}.zip -d {temp_directory}')

        # upload images to Azure AI Search
        images = [os.path.join(temp_directory, image) for image in os.listdir(temp_directory)]
        item_index = dict(video_id=video_id, )
        self.azure_ai_search_wrapper.upload_images(images, item_index, 'keyframes')

    def main_coembeddings_indexing(self):
        # load configuration
        config = load_config()
        working_directory = config['main']['workingDir']

        # list indexed videos
        indexed_videos = self.video_indexer_wrapper.list_all_indexed_videos()

        # download keyframes
        self.video_indexer_wrapper.download_keyframes(indexed_videos, working_directory)

        # generate PromptContent
        for indexed_video in indexed_videos:
            self.video_indexer_wrapper.create_prompt_content(indexed_video)

        # get PromptContent
        prompts_failures = []
        for indexed_video in indexed_videos:
            prompt_content_response = self.video_indexer_wrapper.get_prompt_content(indexed_videos)

            # validate response code 200
            if prompt_content_response.status_code != 200:
                print(f'Failed to get prompt content for video: {indexed_video}')
                prompts_failures.append(indexed_video)
                continue

            with open(f'{working_directory}/{indexed_video}_prompt_content.json', 'w') as f:
                f.write(prompt_content_response.text)

        if prompts_failures:
            print(f'Failed to get prompt content for videos: {prompts_failures}')

        # upload PromptContent to Azure AI Search
        azure_ai_search_wrapper = AzureAISearchWrapper(**config['ais'])
        for file in os.listdir(working_directory):
            video_id = file.split('_')[0]
            if file.endswith('_prompt_content.json'):
                self.upload_texts_to_azure_ai_search(f'{working_directory}/{file}', video_id)

            if file.endswith('.zip'):
                self.index_image_zip(file, video_id, working_directory, azure_ai_search_wrapper)


        stop = 1



if __name__ == '__main__':
    main_coembeddings_indexing()
