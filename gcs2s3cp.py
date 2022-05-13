import argparse
import boto3
import os
from boto3.s3.transfer import TransferConfig
from accession.file import GSFile
from google.cloud.storage.client import Client
from google.cloud.storage.blob import Blob

BOTO3_DEFAULT_MULTIPART_CHUNKSIZE = 8_388_608
BOTO3_MULTIPART_MAX_PARTS = 10_000


def calculate_multipart_chunksize(file_size_bytes: int) -> int:
    """
    Calculates the `multipart_chunksize` to use for `boto3` `TransferConfig` to
    ensure that the file can be uploaded successfully without reaching the 100000
    part limit. The default values are the same as the defaults for `TransferConfig`
    """
    multipart_chunksize = BOTO3_DEFAULT_MULTIPART_CHUNKSIZE * (
        max((file_size_bytes - 1), 0)
        // (BOTO3_MULTIPART_MAX_PARTS * BOTO3_DEFAULT_MULTIPART_CHUNKSIZE)
        + 1
    )
    return multipart_chunksize


def main(args):
    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    source = GSFile(key=args.gs_file_source, name=args.gs_file_source)
    chunk_size = calculate_multipart_chunksize(source.size())
    transfer_config = TransferConfig(multipart_chunksize=chunk_size)
    s3_uri = args.s3_destination_bucket + args.s3_destination_key
    print(f"Uploading {args.gs_file_source} to {s3_uri}")
    client.upload_fileobj(source, args.s3_destination_bucket, args.s3_destination_key, Config=transfer_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gs-file-source",
        type=str,
        help="Source file on GC, for example gs://my_bucket/my_file.txt",
    )
    parser.add_argument(
        "--s3-destination-bucket",
        type=str,
        help="Destination bucket name. For example for destination s3://my_bucket/dir/my_file.txt it is my_bucket",
    )
    parser.add_argument(
        "--s3-destination-key",
        type=str,
        help="Source key on s3, for example s3://my_bucket/dir/my_file.txt it is dir/my_file.txt",
    )
    args = parser.parse_args()
    main(args)
