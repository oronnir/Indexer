"""
Assuming the account has indexed videos, generate PromptContent and download keyframes to index with CLIP.
"""
import json
import os

from config_manager import load_config
from azure_ai_search_wrapper import AzureAISearchWrapper
from video_indexer_wrapper import VideoIndexerWrapper


class CoEmbeddingsIndexer:
    """
    This class is used to index the keyframes of the videos with CLIP embeddings and upload them to Azure AI Search.
    """
    def __init__(self, config, search_k: int = 10):
        self.video_indexer_wrapper = VideoIndexerWrapper(**config['vi'])
        self.azure_ai_search_wrapper = AzureAISearchWrapper(**config['ais'])

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

        # embed images with CLIP
        # embeddings_dicts = [dict(video_id=video_id, clip_embeddings=self.clip_encode_image(image)) for image in range(len(images))]

        item_index = dict(video_id=video_id, start_time=0, end_time=0, )
        self.azure_ai_search_wrapper.upload_images(images, item_index, 'keyframes')

    def main_co_embeddings_indexing(self):
        # load configuration
        config = load_config()
        working_directory = config['main']['workingDir']

        # list indexed videos
        indexed_videos = self.video_indexer_wrapper.list_all_indexed_videos()

        # download keyframes
        self.video_indexer_wrapper.download_keyframes(indexed_videos, working_directory)

        # generate PromptContent
        for indexed_video in indexed_videos:
            self.video_indexer_wrapper.create_prompt_content(indexed_video['id'])

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

    # def clip_encode_image(self, image_path):
    #     """
    #     Embed the image with CLIP.
    #     :param image_path: a path to the image.
    #     :return: the image's CLIP embeddings.
    #     """
    #
    #     # Load your image
    #     image = Image.open(image_path)
    #
    #     # Preprocess the image
    #     preprocessed_image = self.preprocess(image).unsqueeze(0).to('cuda')
    #
    #     # Calculate image features
    #     with torch.no_grad():
    #         image_features = self.model.encode_image(preprocessed_image)
    #
    #     # image_features now contains the embedded representation of the image
    #     return image_features.tolist()

    def fuze_search_results(self, search_results_kf, search_results_pc):
        """
        Fuse the search results from keyframes and prompt content by counting the number of occurrences and updating the
         relevance scores.
        :param search_results_kf:
        :param search_results_pc:
        :return: a unified list of search results.
        """
        # Initialize the fused search results
        fused_search_results = []

        # Count the number of occurrences of each video in the search results
        video_occurrences = {}
        for video in search_results_kf + search_results_pc:
            video_id = video['video_id']
            if video_id in video_occurrences:
                video_occurrences[video_id] += 1
            else:
                video_occurrences[video_id] = 1

        # Update the relevance scores of the search results
        for video in search_results_kf + search_results_pc:
            video_id = video['video_id']
            video['relevance_score'] = video['relevance_score'] * video_occurrences[video_id]
            fused_search_results.append(video)

        # Sort the fused search results by relevance score
        fused_search_results = sorted(fused_search_results, key=lambda x: x['relevance_score'], reverse=True)

        # return the top k search results
        return fused_search_results[:self.search_k]


def run_azure_search_indexing(config):
    co_embedder = CoEmbeddingsIndexer(config)
    co_embedder.main_co_embeddings_indexing()
    print("done indexing...")


def run_query_search(config, search_query):
    co_embedder = CoEmbeddingsIndexer(config)

    # query Azure AI Search
    search_results_kf = co_embedder.azure_ai_search_wrapper.search(search_query, "keyframes")
    print("Search results key frames", search_results_kf)
    search_results_pc = co_embedder.azure_ai_search_wrapper.search(search_query, "prompt_content")
    print("Search results prompt content", search_results_pc)
