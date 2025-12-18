import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")

SOURCE_BUCKET = "bucket-origem"
DEST_BUCKET = "bucket-destino"

SOURCE_PREFIX = "raw/2024/"
DEST_PREFIX = "processed/2024/"

MAX_WORKERS = 10   # número de threads
PAGE_SIZE = 1000   # limite do S3 (máx)

def copy_object(key):
    """
    Copia um objeto do bucket origem para o bucket destino
    alterando o prefixo
    """
    dest_key = key.replace(SOURCE_PREFIX, DEST_PREFIX, 1)

    s3.copy_object(
        Bucket=DEST_BUCKET,
        Key=dest_key,
        CopySource={
            "Bucket": SOURCE_BUCKET,
            "Key": key
        }
    )

    return key


def list_and_copy():
    paginator = s3.get_paginator("list_objects_v2")

    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        for page in paginator.paginate(
            Bucket=SOURCE_BUCKET,
            Prefix=SOURCE_PREFIX,
            PaginationConfig={"PageSize": PAGE_SIZE}
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Submete a task para o pool
                future = executor.submit(copy_object, key)
                futures.append(future)

        # Espera TODAS as cópias terminarem
        for future in as_completed(futures):
            future.result()  # levanta exceção se falhar

    print("Cópia finalizada com sucesso")


if __name__ == "__main__":
    list_and_copy()
