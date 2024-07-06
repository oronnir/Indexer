"""
This file is a wrapper for the Azure AI Search service. It is used to search for similar faces in the database.
The main implementation is to upload the PromptContent to the Azure AI Search service and Images to embedded with CLIP.
"""

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.identity import DefaultAzureCredential


class AzureAISearchWrapper:
    def __init__(self, key, endpoint, index_name, embeddings_mapping):
        self.api_key = key
        self.endpoint = endpoint
        self.embeddings_mapping = embeddings_mapping
        credential = DefaultAzureCredential()
        self.client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)

    def upload_textual_content(self, prompt: str, item_index: dict):
        """
        Uploads the textual content to the Azure AI Search service.

        Args:
            prompt (str): The prompt to be uploaded.
            item_index (object): The index of the item.
        """
        # Prepare the document
        document = item_index
        document["content"] = prompt

        # Upload the document
        result = self.client.upload_documents(documents=[document])

        # Check if the upload was successful
        if not result[0].succeeded:
            print(f"Upload failed: {result[0].error.message}")

    def upload_images(self, images: list[str], item_index: dict, index_name: str):
        """
        Uploads the images to the Azure AI Search service.

        Args:
            images (list[str]): The images to be uploaded.
            item_index (str): The index of the item.
            index_name (str): The name of the index.
        """
        # Create a SearchClient
        search_client = SearchClient(endpoint=self.endpoint,
                                     index_name=index_name,  # use the provided index name
                                     credential=AzureKeyCredential(self.api_key))

        # Prepare the documents
        documents = []
        for image in images:
            document = item_index.copy()  # create a copy of the item_index for each image
            document["image"] = image  # add the image data
            documents.append(document)

        # Upload the documents
        result = search_client.upload_documents(documents=documents)

        # Check if the upload was successful
        for res in result:
            if not res.succeeded:
                print(f"Upload failed: {res.error.message}")


    def search(self, query: str, index_name: str):
        """
        Searches for similar examples in the Azure AI Search service.

        Args:
            query (str): The query to search for.
            index_name (str): The name of the index to search in.
        """
        # Create a SearchClient
        search_client = SearchClient(endpoint=self.endpoint,
                                     index_name=index_name,  # use the provided index name
                                     credential=AzureKeyCredential(self.api_key))

        # Search for similar faces
        response = search_client.search(search_text=query)

        # Get the results
        results = [res for res in response.get_results()]
        return results


class VideoSegmentIndex:
    def __init__(self, location, account_id, video_id, thumbnail_id, start_time, end_time, prompt, image_path, content_vector):
        self.location = location
        self.account_id = account_id
        self.video_id = video_id
        self.segment_id = thumbnail_id
        self.start_time = start_time
        self.end_time = end_time
        self.prompt = prompt
        self.image_path = image_path
        self.content_vector = content_vector

    def to_dict(self):
        return dict(video_id=self.video_id, segment_id=self.segment_id, start_time=self.start_time,
                    end_time=self.end_time, prompt=self.prompt, image_path=self.image_path,
                    content_vector=self.content_vector, location=self.location, account_id=self.account_id)
