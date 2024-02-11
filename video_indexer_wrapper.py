import os
import time
import urllib.request
import json
from typing import List

import requests
from tqdm import tqdm
import urllib.parse
from azure.identity.aio import DefaultAzureCredential
import asyncio


class VideoIndexerWrapper:
    def __init__(self, location=None, account_id=None, subscription_id=None, api_version=None, account_name=None, resource_group_name=None, azure_tenant_id=None):
        self.azure_access_token = None
        self.vi_access_token = None
        self.sleep_time_secs = 0
        self.location = location
        self.account_id = account_id
        self.subscription_id = subscription_id
        self.api_version = api_version
        self.account_name = account_name
        self.resource_group_name = resource_group_name
        self.azure_tenant_id = azure_tenant_id
        self.get_azure_access_token()
        self.get_vi_access_token()

    async def get_azure_access_token_async(self):
        # Azure credentials
        credential = DefaultAzureCredential()

        # get azure access token from async method
        token = await credential.get_token("https://management.azure.com/.default", tenant_id=self.azure_tenant_id)
        self.azure_access_token = f"Bearer {token.token}"

    def get_azure_access_token(self):
        asyncio.run(self.get_azure_access_token_async())

    def get_vi_access_token(self):
        # Request headers
        hdr = {
            'Content-Type': 'application/json',
            'Authorization': self.azure_access_token
        }

        url = f"https://management.azure.com/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group_name}/providers/Microsoft.VideoIndexer/accounts/{self.account_name}/generateAccessToken?api-version={self.api_version}"

        # adding body
        body = {
            "permissionType": "Contributor",
            "scope": "Account"
        }

        body = json.dumps(body)
        body = body.encode('utf-8')
        req = urllib.request.Request(url, data=body, headers=hdr, method='POST')
        req.get_method = lambda: 'POST'
        response = urllib.request.urlopen(req)
        response_access_token_bytes = response.read()
        response_access_token = json.loads(response_access_token_bytes.decode())
        access_token = response_access_token["accessToken"]
        self.vi_access_token = access_token
        return

    def renew_access_tokens(self):
        self.get_azure_access_token()
        self.get_vi_access_token()

    def list_videos_single_page(self, next_page_skip=None) -> dict:
        # list videos
        videos = []
        try:
            url = f"https://api.videoindexer.ai/{self.location}/Accounts/{self.account_id}/Videos?accessToken={self.vi_access_token}"
            if next_page_skip is not None:
                url += f"&skip={next_page_skip}"

            hdr = {
                'Cache-Control': 'no-cache',
                'Ocp-Apim-Subscription-Key': self.subscription_id,
            }

            req = urllib.request.Request(url, headers=hdr)

            req.get_method = lambda: 'GET'
            response = urllib.request.urlopen(req)
            if response.getcode() != 200:
                print(response.getcode())
                print(f'Error: {response.getcode()}')

            videos = json.loads(response.read())

        except Exception as e:
            print(e)
        return videos

    def get_video_index(self, video_id):
        video_index = None

        try:
            url = f"https://api.videoindexer.ai/{self.location}/Accounts/{self.account_id}/Videos/{video_id}/Index?accessToken={self.vi_access_token}"
            headers = {
                'Cache-Control': 'no-cache',
                'Ocp-Apim-Subscription-Key': self.subscription_id
            }

            req = urllib.request.Request(url, headers=headers)
            req.get_method = lambda: 'GET'
            response = urllib.request.urlopen(req)
            response_code = response.getcode()
            video_index = json.loads(response.read())

            if response_code != 200:
                print(f'Error: response code: {response_code}, video_id: {video_id}, response: {video_index}')
        except Exception as e:
            print(e)
        return video_index

    def list_all_indexed_videos(self):
        """
        list all indexed videos and download their thumbnails
        :return:
        """
        videos = self.list_videos_single_page()
        all_indexed_videos = [videos]
        while videos is not None and \
                'nextPage' in videos and \
                'done' in videos['nextPage'] and \
                videos['nextPage']['done'] is False:
            videos = self.list_videos_single_page()
            all_indexed_videos.append(videos)
        return all_indexed_videos

    def get_n_unknown_face_ids(self, n_unknown_face_ids, working_dir):
        """
        list videos page by page and get their video index while counting the number of unknown face ids
        :return: first n unknown face ids
        """
        unknown_face_ids = []
        video_face_impressions = []

        # load existing video face impressions
        for file in os.listdir(working_dir):
            if file.endswith(".jpg"):
                thumbnail_id = os.path.basename(file).split('.')[0]
                unknown_face_ids.append(thumbnail_id)

        skip_batch = 200
        batch_counter = 1
        # self.renew_access_tokens()
        videos = self.list_videos_single_page()
        bar = tqdm(total=n_unknown_face_ids)
        while len(unknown_face_ids) < n_unknown_face_ids:
            bar.update(len(unknown_face_ids))
            batch_counter += 1
            for video in videos['results']:
                video_id = video['id']
                video_thumbnail_ids_json = os.path.join(working_dir, f'{video_id}.json')
                if os.path.exists(video_thumbnail_ids_json):
                    continue

                # sleep to avoid throttling
                time.sleep(self.sleep_time_secs)

                # get video index and check if it has faces
                video_index = self.get_video_index(video_id)

                # retry once if access token expired
                if video_index is None:
                    self.renew_access_tokens()
                    video_index = self.get_video_index(video_id)

                if video_index is None or \
                    'videos' not in video_index or \
                        len(video_index['videos'][0]) == 0 or \
                        video_index['videos'][0] is None or \
                        'insights' not in video_index['videos'][0] or \
                        video_index['videos'][0]['insights'] is None or \
                        'faces' not in video_index['videos'][0]['insights']:
                    continue

                faces = video_index['videos'][0]['insights']['faces']
                video_impressions = VideoFaceImpressions(faces, video_id)
                video_face_impressions.append(video_impressions)
                unknown_face_ids.extend(video_impressions.unknown_face_thumbnail_ids)

                # serialize video face thumbnail ids to json
                with open(video_thumbnail_ids_json, 'w') as f:
                    f.write(video_impressions.tojson())

                # download all thumbnails
                self.get_video_face_impressions(video['id'], working_dir, video_impressions.unknown_face_thumbnail_ids)
                if len(unknown_face_ids) >= n_unknown_face_ids:
                    break
            if len(unknown_face_ids) >= n_unknown_face_ids:
                break
            if videos is not None and \
                    'nextPage' in videos and \
                    'done' in videos['nextPage'] and \
                    videos['nextPage']['done'] is False:
                videos = self.list_videos_single_page(skip_batch*batch_counter)
            else:
                print(f'Failed to get next page of videos: {videos}. Stopping.')
                break

        return unknown_face_ids

    def get_video_indexer_thumbnail_api(self, video_id, thumbnail_id, target_folder_path: str):
        """
        Get video face impression for the given face thumbnail id
        :param target_folder_path:
        :param video_id:
        :param thumbnail_id: a GUID from to Video Indexer
        :return: the image path of the thumbnail
        """
        file_path = f'{target_folder_path}/{thumbnail_id}.jpg'
        if os.path.exists(file_path):
            return file_path

        url = f"https://api.videoindexer.ai/{self.location}/Accounts/{self.account_id}/Videos/{video_id}/Thumbnails/{thumbnail_id}?accessToken={self.vi_access_token}"
        headers = {
            # Request headers
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': self.subscription_id,
        }

        response_code = None
        try:
            req = urllib.request.Request(url, headers=headers)
            req.get_method = lambda: 'GET'
            response = urllib.request.urlopen(req)
            response_code = response.getcode()
            video_indexer_thumbnail_id = response.read()

            # save the thumbnail to an image file
            with open(file_path, 'wb') as f:
                f.write(video_indexer_thumbnail_id)
        except Exception as e:
            print(e)

        if response_code != 200:
            print(f'Error: {response_code}')
            return None
        return file_path

    def get_video_face_impressions(self, video_id: str, target_folder_path: str, face_thumbnail_ids: List[str]):
        """
        Get video face impressions for the given face thumbnail ids
        :param target_folder_path:
        :param video_id:
        :param face_thumbnail_ids: a list of GUID values known to Video Indexer of thumbnail ids
        :return:
        """
        for thumbnail_id in face_thumbnail_ids:
            image_path = self.get_video_indexer_thumbnail_api(video_id, thumbnail_id, target_folder_path)
            if image_path is None:
                self.renew_access_tokens()
                image_path = self.get_video_indexer_thumbnail_api(video_id, thumbnail_id, target_folder_path)
            if image_path is None:
                print(f'Failed to download thumbnail {thumbnail_id} for video {video_id}')
                continue

    def upload_video(self, video_path, privacy='Private', priority='Low', language='auto', indexing_preset='Default', streaming_preset='Default', send_success_email='false', use_managed_identity_to_download_video='false', prevent_duplicates='false'):
        """
        Upload the video to the Azure Video Indexer
        :param video_path:
        :return:
        """
        video_name = os.path.basename(video_path)

        # Open the video file in binary mode
        with open(video_path, 'rb') as video_file:
            video_data = video_file.read()

        # Make the POST request to the API
        endpoint_url = f"https://api.videoindexer.ai/{self.location}/Accounts/{self.account_id}/Videos?name={video_name}&privacy={privacy}&priority={priority}&language={language}&fileName=&indexingPreset={indexing_preset}&streamingPreset={streaming_preset}&sendSuccessEmail={send_success_email}&useManagedIdentityToDownloadVideo={use_managed_identity_to_download_video}&preventDuplicates={prevent_duplicates}&accessToken={self.vi_access_token}"
        response = requests.post(endpoint_url, files={"file": video_data})

        # return the response
        return response.json()

    def get_video_artifacts(self, video_id, artifact_type:str='KeyframesThumbnails'):
        """
        Get the artifacts of the video
        :param video_id:
        :return:
        """
        # Make the GET request to the API
        endpoint_url = f"https://api.videoindexer.ai/{self.location}/Accounts/{self.account_id}/Videos/{video_id}/ArtifactUrl?type={artifact_type}&accessToken={self.vi_access_token}"
        response = requests.get(endpoint_url)

        # return the response
        return response

    def download_keyframes(self, indexed_videos, working_directory):
        """
        Download videos' artifacts, unzip, copy keyframes and delete zip
        :param indexed_videos:
        :param working_directory:
        :return:
        """
        kf_video_id_to_zip = dict()
        failed_video_ids = []

        # download artifacts
        for page in indexed_videos:
            for video in page['results']:
                video_id = video['id']
                artifacts_response = self.get_video_artifacts(video_id, 'KeyframesThumbnails')
                if artifacts_response.status_code != 200:
                    print(f'Error: {artifacts_response.status_code}')
                    continue
                kf_url = artifacts_response.json()

                # download keyframes zip from url to working directory
                zip_file_path = os.path.join(working_directory, f'{video_id}.zip')
                urllib.request.urlretrieve(kf_url, zip_file_path)

                if os.path.exists(zip_file_path):
                    print(f'Downloaded keyframes zip for video {video_id} to {zip_file_path}')
                    kf_video_id_to_zip[video_id] = zip_file_path
                else:
                    print(f'Failed to download keyframes zip for video {video_id} to {zip_file_path}')
                    failed_video_ids.append(video_id)
                    continue
        print(f'Done downloading all keyframes for {len(kf_video_id_to_zip)} videos. Failed to download keyframes for {len(failed_video_ids)} videos')
        return kf_video_id_to_zip, failed_video_ids

    def create_prompt_content(self, video_id):
        import urllib.request

        try:
            url = f"https://api.videoindexer.ai/eastus/Accounts/{self.account_id}/Videos/{video_id}/PromptContent"

            hdr = {
                # Request headers
                'Cache-Control': 'no-cache',
                'Ocp-Apim-Subscription-Key': self.subscription_id,
            }

            req = urllib.request.Request(url, headers=hdr)

            req.get_method = lambda: 'POST'
            response = urllib.request.urlopen(req)
            print(response.getcode())
            print(response.read())
        except Exception as e:
            print(e)

    def get_prompt_content(self, video_id):
        """
        Get the prompt content of the video
        :param video_id:
        :return:
        """
        import urllib.request

        try:
            url = f"https://api.videoindexer.ai/trial/Accounts/{self.account_id}/Videos/{video_id}/PromptContent?accessToken={self.vi_access_token}"

            hdr = {
                # Request headers
                'Cache-Control': 'no-cache',
                'Ocp-Apim-Subscription-Key': self.subscription_id,
            }

            req = urllib.request.Request(url, headers=hdr)

            req.get_method = lambda: 'GET'
            response = urllib.request.urlopen(req)
            return response
        except Exception as e:
            print(e)


class VideoFaceImpressions:
    def __init__(self, faces, video_id):
        self.video_id = video_id
        self.faces = faces
        self.all_thumbnail_ids = []
        self.known_face_thumbnail_ids = []
        self.unknown_face_thumbnail_ids = []
        self.known_person_ids = []
        for face in faces:
            self.all_thumbnail_ids.append(face['thumbnailId'])
            if 'Unknown' in face['name']:
                self.unknown_face_thumbnail_ids.append(face['thumbnailId'])
            elif 'knownPersonId' not in face:
                continue
            else:
                self.known_person_ids.append(face['knownPersonId'])
                self.known_face_thumbnail_ids.append(face['thumbnailId'])

    def tojson(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
