import concurrent.futures
import subprocess
import os
import time
from datetime import datetime

from Utils import Semaphore
from video_indexer_wrapper import VideoIndexerWrapper


class Streamer:
    def __init__(self, streamlink_id: int, stream_url, video_repo, video_duration):
        self.STREAMURL = stream_url
        self.VIDEO_REPO = video_repo
        self.VIDEO_DURATION = video_duration
        self.streamlink_path = fr"C:\...\streamlink{streamlink_id}\streamlink.exe"  # streamlink for mac

    def stream_video_and_chunk_files(self):
        # Get the current date and time
        now = datetime.now()

        # Format as a string
        now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
        output_video_path = os.path.join(self.VIDEO_REPO, f"{now_str}.ts")
        command = f'{self.streamlink_path} --hls-duration {self.VIDEO_DURATION} "{self.STREAMURL}" 360p -o {output_video_path}'

        # Use subprocess.run() to execute the command  and store the result
        exit_code = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if exit_code.returncode != 0:
            raise Exception(f'failed serialize stream with exit code {exit_code}')


def serialize_live_stream(stream_url: str, video_duration: str, total_seconds: int, video_repo: str, num_iterations: int):
    """
    This function streams a live video and serializes the video to the Azure Video Indexer.
    To avoid stream loss during encoding, use a Semaphore to chunk the video files.
    :return:
    """
    processors = Semaphore(2)
    for i in range(num_iterations):
        # Your code here
        print(f"Iteration {i + 1}")

        # Acquire the semaphore
        start_time = time.time()
        streamlink_id = (i % 2) + 1

        # Stream the video and chunk the files
        streamer = Streamer(streamlink_id, stream_url, video_repo, video_duration)
        res = processors.parallelize([dict()], streamer.stream_video_and_chunk_files)

        end_time = time.time()

        # Wait for 5 seconds before the next iteration
        streamming_time = end_time - start_time
        if streamming_time < total_seconds:
            time.sleep(total_seconds - streamming_time)


def upload_videos_to_video_indexer(location, account_id, subscription_id, api_version, account_name, total_seconds,
                                   resource_group_name, azure_tenant_id, video_folder_path, num_iterations):
    """
    This function uploads the video to the Azure Video Indexer.
    :param location:
    :param account_id:
    :param subscription_id:
    :param api_version:
    :param account_name:
    :param resource_group_name:
    :param azure_tenant_id:
    :param video_folder_path:
    :param num_iterations:
    :return:
    """
    vi_wrapper = VideoIndexerWrapper(location=location, account_id=account_id, subscription_id=subscription_id,
                                     api_version=api_version, account_name=account_name,
                                     resource_group_name=resource_group_name, azure_tenant_id=azure_tenant_id)

    # Wait for the first video to be chunked
    init_sleep_secs = 1 + 4*total_seconds
    print(f"Waiting for the first video to be chunked - Going to sleep for a {init_sleep_secs} seconds.")
    time.sleep(init_sleep_secs)
    print("Waking up from the first sleep.")

    # identify the video file
    for i in range(num_iterations):
        for video_ts in os.listdir(video_folder_path):
            if not video_ts.endswith(".ts"):
                continue

            # upload to video indexer
            video_path = os.path.join(video_folder_path, video_ts)
            print(f"Uploading {video_path} to the Azure Video Indexer.")
            response = vi_wrapper.upload_video(video_path)

            # Print the response
            print(response)

            # Delete the video file
            os.remove(video_path)
        time.sleep(1)
    print("All videos have been uploaded to the Azure Video Indexer.")


def run_download_and_upload(stream_url, video_duration, video_repo, num_iterations, config):
    """
    This function runs the download and upload of the video.
    :param stream_url:
    :param video_duration:
    :return:
    """
    num_files_in_working_repo = len(os.listdir(video_repo))
    if num_files_in_working_repo > 0:
        print(f"Working directory {video_repo} is not empty. Please clear the directory and try again.")
        return

    # The total number of seconds is calculated by converting hours to seconds (hours * 3600),
    # adding the conversion of minutes to seconds (minutes * 60), and then adding the seconds.
    hours, minutes, seconds = [int(part) for part in video_duration.split(":")]
    total_seconds = hours * 3600 + minutes * 60 + seconds

    vi_config = config['vi']
    vi_config['video_folder_path'] = video_repo
    vi_config['num_iterations'] = num_iterations
    vi_config['total_seconds'] = total_seconds

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_one = executor.submit(serialize_live_stream, stream_url, video_duration, total_seconds, video_repo,
                                     num_iterations)
        future_two = executor.submit(upload_videos_to_video_indexer, **vi_config)

        concurrent.futures.wait([future_one, future_two], return_when=concurrent.futures.ALL_COMPLETED)

        # Check for exceptions
        if future_one.exception():
            print(f"Exception in serialize_live_stream: {future_one.exception()}")
        if future_two.exception():
            print(f"Exception in upload_videos_to_video_indexer: {future_two.exception()}")

    print("Both functions have completed.")
