"""recorded uploader."""
from typing import Generator, List
import os
import re
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import mojimoji
import psutil
import boto3
from boto3.s3.transfer import TransferConfig
from mypy_boto3_s3.client import S3Client


RECORDED_PATHS = [
    '/app/cache_recorded',  # /mnt/evo850/recorded
    '/app/recorded',        # /mnt/hgst4tb/recorded
    '/app/recorded2',       # /mnt/wd6tb1/recorded
    '/app/recorded3',       # /mnt/wd8tb1/recorded
    '/app/recorded4',       # /mnt/wd8tb2/recorded
]

KB = 1024
MB = KB * KB


class S3:
    """S3 Client."""

    def __init__(self) -> None:
        self.bucket_name: str = os.getenv('S3_BUCKET_NAME', '')
        self.client: S3Client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION'),
        )

    def upload_file(self, upload_file_path: str, prefix: str):
        """Upload file.

        Args:
            upload_file_path (str): Upload file path
            prefix (str): S3 key prefix
        """
        config = TransferConfig(
            max_concurrency=20,
            multipart_chunksize=30 * MB,
        )
        extra_args = {
            'StorageClass': 'DEEP_ARCHIVE'
        }
        with tqdm(total=os.path.getsize(upload_file_path), unit='B', unit_scale=True) as progress:
            self.client.upload_file(
                upload_file_path,
                self.bucket_name,
                str(os.path.join(prefix, os.path.basename(
                    upload_file_path))).replace('\\', '/'),
                Callback=lambda bytes_transferred: progress.update(
                    bytes_transferred),
                Config=config,
                ExtraArgs=extra_args,
            )


class RecordedHandler:
    """Recorded handler."""

    @classmethod
    def search(cls, title: str) -> Generator[str, None, None]:
        """search.

        Args:
            title (str): Anime title

        Yields:
            Generator[str]: recorded path
        """
        pattern = re.compile(
            r'' + title + '|' + mojimoji.han_to_zen(title) + r'',
            re.IGNORECASE
        )

        for recorded_path in RECORDED_PATHS:
            for all_file_path in Path(recorded_path).glob('*'):
                if pattern.search(str(all_file_path)):
                    yield str(all_file_path)

    @staticmethod
    def check_disk_free_space():
        """Check the free disk space in gigabytes."""
        for recorded_path in RECORDED_PATHS:
            disk_usage = psutil.disk_usage(recorded_path)
            print(f'{recorded_path}: {round(disk_usage.free/1000/1000/1000, 1)} GB')

    @staticmethod
    def get_file_size(file_path: str) -> float:
        """Get file size. N.N GB"""
        return round(os.path.getsize(file_path)/1000/1000/1000, 1)

    @staticmethod
    def delete_local_recorded(file_paths: List[str]):
        for file_path in file_paths:
            os.remove(file_path)


def input_yes_no() -> bool:
    """yes No input."""
    inp = input('y/N: ')
    if inp == 'y':
        return True
    else:
        return False


if __name__ == "__main__":
    commands = [
        '1: Check disk free space',
        '2: Search recorded',
        '3: Upload recorded',
        '4: Delete recorded',
    ]
    command = input('Select a command\n' + '\n'.join(commands) + '\n')
    load_dotenv()

    if command == '1':
        RecordedHandler.check_disk_free_space()

    elif command == '2':
        for path in RecordedHandler.search(input('title: ')):
            print(f'{path} {RecordedHandler.get_file_size(path)} GB')

    elif command == '3':
        prefix = input('S3 prefix ex:2022Q3/Engage Kiss/ : ')
        title = input('title: ')
        s3 = S3()
        for path in RecordedHandler.search(title):
            s3.upload_file(path, prefix)

    elif command == '4':
        title = input('title: ')
        delete_file_paths = [path for path in RecordedHandler.search(title)]
        newline = '\n'
        print(
            f'Are you sure you want to delete this?\n{newline.join(delete_file_paths)}')
        if input_yes_no():
            RecordedHandler.delete_local_recorded(delete_file_paths)

    else:
        print(f'This command not exist. {command}')
