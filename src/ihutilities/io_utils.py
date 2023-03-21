#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals


import codecs
import csv
import dataclasses
import fnmatch
import hashlib
import io
import logging
import os
import shutil
import time
import zipfile
from typing import Any, Dict, List, Optional, Tuple, TextIO, Generator
from pathlib import Path

import requests
import glob

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

config = Config(connect_timeout=900, read_timeout=900, retries={"max_attempts": 3})


def create_s3_client(
    profile_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> boto3.session.Session.client:
    if os.getenv("AWS_PROFILE") not in ["", None] and profile_name is None:
        profile_name = os.getenv("AWS_PROFILE")
    if profile_name is not None:
        session = boto3.Session(profile_name=profile_name)
    elif s3_session is not None:
        session = s3_session
    elif aws_access_key_id is not None and aws_secret_access_key is not None:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )
    else:
        session = boto3.Session()

    client = session.client("s3", config=config)
    return client


def is_s3(file_uri: str) -> bool:
    is_s3_uri = False
    if file_uri.lower().startswith("s3://"):
        is_s3_uri = True
    return is_s3_uri


def join_paths_local_or_s3(*path_parts: str) -> str:
    full_path = ""
    if is_s3(path_parts[0]):
        full_path = "/".join(path_parts[1:])
        full_path = full_path.replace("//", "/")
        if path_parts[0].endswith("/"):
            full_path = path_parts[0] + full_path
        else:
            full_path = path_parts[0] + "/" + full_path
    else:
        full_path = os.path.join(*path_parts)

    return full_path


def is_file_local_or_s3(file_uri: str):
    is_file = False
    if is_s3(file_uri):
        matching_files = list_files_local_or_s3(file_uri)
        if len(matching_files) == 1:
            is_file = True
    else:
        is_file = os.path.isfile(file_uri)

    return is_file


def split_uri(full_uri: str) -> Tuple[str, str, str]:
    bucket = ""
    key = ""
    filename = ""
    if is_s3(full_uri):
        full_uri = full_uri.replace("s3://", "")
        parts = full_uri.split("/")
        bucket = parts[0]
        if len(parts) >= 2:
            filename = parts[-1]
            key = "/".join(parts[1:])
        else:
            filename = ""
            key = ""
        bucket = "s3://" + bucket
    else:
        bucket = str(Path(full_uri).parents[0])
        key = ""
        filename = Path(full_uri).name

    return bucket, key, filename


def write_to_file(
    file_uri: str,
    content: str,
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> str:

    status = ""

    if is_s3(file_uri):
        s3_bucket, key, _ = split_uri(file_uri)
        status = _write_file_to_s3(
            key,
            s3_bucket,
            content,
            aws_profile=aws_profile,
            s3_session=s3_session,
        )
    else:
        status = _write_file_to_local(
            file_uri,
            content,
        )

    return status


def _write_file_to_local(
    filepath: str,
    content: str,
    encoding: str = "utf-8",
) -> str:

    status = ""

    if os.path.isfile(filepath):
        status = f"Deleting existing existing {filepath}, before creating new file"
        os.remove(filepath)
    else:
        status = f"Creating new file: {filepath}"

    with open(filepath, "w", encoding=encoding) as documentation_file:
        documentation_file.write(content)

    return status


def _write_file_to_s3(
    key: str,
    s3_bucket: str,
    content: str,
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> str:

    s3_bucket = s3_bucket.replace("s3://", "")
    s3_client = create_s3_client(profile_name=aws_profile, s3_session=s3_session)
    objs = list(s3_client.list_objects(Bucket=s3_bucket, Prefix=key))
    filepath = f"s3://{s3_bucket}/{key}"
    if "Contents" in objs:
        status = f"Deleting existing existing {filepath}, before creating new file"
        s3_client.delete_object(Bucket=s3_bucket, Key=key)
    else:
        status = f"Creating new file: {filepath}"

    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)

    return status


def write_dictionary(
    file_uri: str,
    data: List[Dict[str, Any]],
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
    append: bool = True,
    delimiter: str = ",",
    quoting: int = csv.QUOTE_MINIMAL,
    encoding: str = "utf-8",
) -> str:

    status = ""
    if len(data) == 0:
        raise IndexError("write_dictionary was supplied with an empty list")

    if dataclasses.is_dataclass(data[0]):
        temp_data = []
        for row in data:
            temp_data.append(dataclasses.asdict(row))
        data = temp_data
    try:
        _ = data[0].keys()
    except AttributeError as attribute_exception:
        raise AttributeError(
            "write_dictionary was not supplied with a list of dictionaries"
        ) from attribute_exception

    if is_s3(file_uri):
        s3_bucket, key, _ = split_uri(file_uri)
        status = _write_dictionary_to_s3(
            key,
            s3_bucket,
            data,
            aws_profile=aws_profile,
            s3_session=s3_session,
            append=append,
            delimiter=delimiter,
            quoting=quoting,
        )
    else:
        status = _write_dictionary_to_local_file(
            file_uri,
            data,
            append=append,
            delimiter=delimiter,
            quoting=quoting,
            encoding=encoding,
        )

    return status


def _write_dictionary_to_local_file(
    filepath: str,
    data: List[Dict[str, Any]],
    append: bool = True,
    delimiter: str = ",",
    quoting: int = csv.QUOTE_MINIMAL,
    encoding: str = "utf-8",
) -> str:

    status = ""
    keys = data[0].keys()

    newfile = not os.path.isfile(filepath)

    status = _make_write_dictionary_status(append, filepath, newfile)
    if not append and not newfile:
        os.remove(filepath)
        newfile = True

    with open(filepath, "a", encoding=encoding, errors="ignore") as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            keys,
            lineterminator="\n",
            delimiter=delimiter,
            quoting=quoting,
            escapechar="\\",
        )
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(data)

    return status


def _write_dictionary_to_s3(
    key: str,
    s3_bucket: str,
    data: List[Dict[str, Any]],
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
    append: bool = True,
    delimiter: str = ",",
    quoting: int = csv.QUOTE_MINIMAL,
) -> str:

    s3_bucket = s3_bucket.replace("s3://", "")
    s3_client = create_s3_client(profile_name=aws_profile, s3_session=s3_session)
    objs = list(s3_client.list_objects(Bucket=s3_bucket, Prefix=key))
    if "Contents" in objs:
        newfile = False
    else:
        newfile = True

    filepath = f"s3://{s3_bucket}/{key}"
    status = _make_write_dictionary_status(append, filepath, newfile)
    if not append and not newfile:
        s3_client.delete_object(Bucket=s3_bucket, Key=key)
        newfile = True

    stream = io.StringIO()
    headers = list(data[0].keys())
    writer = csv.DictWriter(
        stream, fieldnames=headers, lineterminator="\n", delimiter=delimiter, quoting=quoting
    )
    output_data = None
    if newfile:
        writer.writeheader()
        writer.writerows(data)
        output_data = stream.getvalue()
    else:
        old_data = s3_client.get_object(Bucket=s3_bucket, Key=key)["Body"].read().decode("utf-8")
        writer.writerows(data)
        new_data = stream.getvalue()
        output_data = old_data + new_data

    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=output_data)

    return status


def _make_write_dictionary_status(append: bool, filepath: str, newfile: bool) -> str:
    status = ""
    if not append and not newfile:
        status = f"Append is False, and {filepath} exists therefore file is being deleted"
    elif not newfile and append:
        status = f"Append is True, and {filepath} exists therefore data is being appended"
    else:
        status = f"New file {filepath} is being created"
    return status


def calculate_file_sha(filepath: str, encoding="utf-8-sig"):
    file_sha = hashlib.sha1()

    # Switched this to get sha calculation for files within zip files working
    # fh = file_handle_or_none(filepath, encoding=None, mode="rb")
    fh = get_a_file_handle(filepath, encoding=encoding, mode="rb", zip_guess=True)

    if fh is None:
        return None

    if ".zip" not in filepath.lower():
        file_size = os.path.getsize(filepath)
    else:
        zip_path, name_in_zip = split_zipfile_path(filepath)
        if (
            (name_in_zip is not None)
            and (zip_path is not None)
            and len(name_in_zip) != 0
            and "*" not in name_in_zip
        ):
            zf = zipfile.ZipFile(zip_path)
            file_size = zf.getinfo(name_in_zip).file_size
        else:  # if no name in zip is specified then calculate the sha of the zip file as a whole
            file_size = os.path.getsize(zip_path)
            fh = open(zip_path, "rb")

    # This magic should make our sha match the git sha
    file_sha.update("blob {:d}\0".format(file_size).encode("utf-8"))

    # with open(filepath, "rb") as f:
    with fh:
        for chunk in iter(lambda: fh.read(4096), b""):
            file_sha.update(chunk)

    return file_sha.hexdigest()


def get_a_file_handle(
    file_path: str,
    encoding: Optional[str] = "utf-8-sig",
    mode: Optional[str] = "r",
    zip_guess: Optional[bool] = True,
):
    """This function returns a file handle, even if a file is within a zip

    Args:
        file_path (str):
            file path which may point to a file inside a zip

    Keyword args:
        encoding (str):
            character encoding of the taregt file
        mode (str):
            mode to use for opening file
        zip_guess (bool):
            if True then we try to guess whether the file is a zip

    Returns:
       a file handler

    Example:
        >>>
    """
    # If we have a straightforward file then return that
    fh = None
    if file_path is None:
        return fh

    if ".zip" not in file_path.lower():
        if mode == "r":
            fh = file_handle_or_none(file_path, encoding=encoding, mode=mode)
        else:  # This is what we do for binary files, no encoding permitted here
            fh = file_handle_or_none(file_path, encoding=None, mode=mode)
    else:
        zip_path, name_in_zip = split_zipfile_path(file_path)
        zf = zipfile.ZipFile(zip_path)
        namelist = zf.namelist()

        if len(name_in_zip) == 0:
            try:
                cf = zf.open(namelist[0], "r")
            except (NotImplementedError, OSError):
                raise
            if mode == "r":
                fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
            else:
                fh = io.BytesIO(cf.read())
        else:
            for name in namelist:
                if fnmatch.fnmatch(name, name_in_zip):
                    try:
                        cf = zf.open(name, "r")
                    except (NotImplementedError, OSError):
                        raise
                    if mode == "r":
                        fh = io.TextIOWrapper(io.BytesIO(cf.read()), encoding=encoding)
                    else:
                        fh = io.BytesIO(cf.read())

    return fh


def file_handle_or_none(file_path, encoding="utf-8-sig", mode="r") -> Any:
    try:
        if encoding is not None:
            fh = open(file_path, encoding=encoding, mode=mode)
        else:
            fh = open(file_path, mode=mode)
    except FileNotFoundError:
        fh = None
    return fh


def split_zipfile_path(zipfile_path: str) -> Tuple[Any, Any]:
    if zipfile_path is None:
        return None, None
    if ".zip" not in zipfile_path.lower():
        zip_path = zipfile_path
        name_in_zip = ""
        return zip_path, name_in_zip

    if ".zip" in zipfile_path:
        parts = zipfile_path.split(".zip")
        zip_path = parts[0] + ".zip"
    else:
        parts = zipfile_path.split(".ZIP")
        zip_path = parts[0] + ".ZIP"

    if len(parts) != 0 and len(parts) == 2:
        name_in_zip = parts[1][1:]
    else:
        name_in_zip = ""

    return zip_path, name_in_zip


def download_file_from_url(url: str, local_filepath: str) -> str:
    """
    A function to download and save a file from a url

    Args:
        url (str): url of file to download
        local_filepath (str): local file path to which to save

    Returns:
        local_filepath (str): the path to which the file was saved
    """
    t0 = time.time()
    logger.info("Downloading file from {}, saving to {}".format(url, local_filepath))

    if not os.path.isdir(os.path.dirname(local_filepath)):
        os.makedirs(os.path.dirname(local_filepath))

    if os.path.isfile(local_filepath):
        logger.warning(
            "Local file {} already exists, delete to download again".format(local_filepath)
        )
        return local_filepath

    # NOTE the stream=True parameter
    try:
        r = requests.get(url, stream=True, timeout=3.5)
    except:
        time.sleep(5)
        logger.warning("Connection to {} failed on first try, making second attempt".format(url))
        r = requests.get(url, stream=True, timeout=3.5)

    chunk_count = 0
    tmp_path = local_filepath + "_tmp"
    with open(tmp_path, "wb") as f:
        for i, chunk in enumerate(r.iter_content(chunk_size=1024)):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                chunk_count += 1
                # f.flush() commented by recommendation from J.F.Sebastian
                if (i % 1000) == 0:
                    print(".", end="", flush=True)
    print("", flush=True)
    t1 = time.time()
    shutil.copy(tmp_path, local_filepath)
    os.remove(tmp_path)

    logger.info("Download took {:.2f}seconds for {:.2f}mb\n".format(t1 - t0, chunk_count / 1024))

    return local_filepath


def iterator_from_filepath(
    file_path: str,
    delimiter: str = ",",
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> Generator[Dict, None, None]:
    if is_s3(file_path):
        bucket_name, key, _ = split_uri(file_path)
        bucket_name = bucket_name.replace("s3://", "")
        s3_client = create_s3_client(profile_name=aws_profile, s3_session=s3_session)
        try:
            file_handle = s3_client.get_object(Bucket=bucket_name, Key=key)
        except s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"{bucket_name}/{key} not found")

        for row in csv.DictReader(
            codecs.getreader("utf-8")(file_handle["Body"]), delimiter=delimiter
        ):
            yield row
    else:
        with open(os.path.join(file_path), encoding="utf-8-sig") as file_handle:
            reader = csv.DictReader(file_handle, delimiter=delimiter)
            for row in reader:
                yield row


def file_handle_from_filepath(
    file_path: str,
    as_bytes: bool = False,
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> TextIO:
    file_handle = None
    if is_s3(file_path):
        bucket_name, key, file_ = split_uri(file_path)
        bucket_name = bucket_name.replace("s3://", "")
        s3_client = create_s3_client(profile_name=aws_profile, s3_session=s3_session)
        try:
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
        except s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Bucket name: {bucket_name} with key: {key} not found")

        file_handle = codecs.getreader("utf-8")(s3_object["Body"])
    else:
        if as_bytes:
            file_handle = open(os.path.join(file_path), "rb")
        else:
            file_handle = open(os.path.join(file_path), encoding="utf-8-sig")

    return file_handle


def list_files_local_or_s3(
    file_pattern: str,
    aws_profile: Optional[str] = None,
    s3_session: Optional[boto3.session.Session] = None,
) -> List[str]:
    file_list = []
    if is_s3(file_pattern):
        s3_bucket, _, _ = split_uri(file_pattern)
        bucket_name = s3_bucket.replace("s3://", "")
        pattern = file_pattern.replace("s3://" + bucket_name + "/", "")
        subdirectory_search = False
        if pattern.endswith("/"):
            subdirectory_search = True
            pattern += "*"

        s3_client = create_s3_client(profile_name=aws_profile, s3_session=s3_session)
        list_object_response = s3_client.list_objects_v2(Bucket=bucket_name)
        if list_object_response["IsTruncated"]:
            print(
                f"Warning: list_objects_local_or_s3 returned truncated results for {s3_bucket}",
                flush=True,
            )
        for object_key in list_object_response["Contents"]:

            if fnmatch.fnmatch(object_key["Key"], pattern):
                if subdirectory_search:
                    full_path = join_paths_local_or_s3(s3_bucket, object_key["Key"].split("/")[0])
                else:
                    full_path = join_paths_local_or_s3(s3_bucket, object_key["Key"])
                file_list.append(full_path)
    else:
        file_list = glob.glob(file_pattern)

    return file_list


def expand_file_path(examplefile: Optional[str]) -> Optional[str]:
    if examplefile is None:
        return None
    if not is_s3(examplefile):
        if examplefile == "":
            return os.getcwd()

        if not Path(examplefile).is_absolute():
            example_path_full = os.path.join(os.getcwd(), examplefile)
        else:
            example_path_full = examplefile
    else:
        example_path_full = examplefile

    return example_path_full
