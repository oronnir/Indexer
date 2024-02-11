"""
This file is a wrapper for the Azure AI Search service. It is used to search for similar faces in the database.
The main implementation is to upload the PromptContent to the Azure AI Search service and Images to embedded with CLIP.
"""


class AzureAISearchWrapper:
    def __init__(self, **kwargs):
        self.api_key = kwargs
        self.endpoint = kwargs

    def upload_textual_content(self, prompt: str, item_index: dict):
        """
        Uploads the textual content to the Azure AI Search service.

        Args:
            prompt (str): The prompt to be uploaded.
            item_index (object): The index of the item.
        """
        pass

    def upload_images(self, images: list[str], item_index: str, index_name: str):
        """
        Uploads the images to the Azure AI Search service.

        Args:
            images (list[str]): The images to be uploaded.
            item_index (str): The index of the item.
            index_name (str): The name of the index.
        """
        pass
