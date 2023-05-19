import os
import re
import subprocess
import urllib.request
from urllib.error import HTTPError, URLError
import json


# A script to collect which dockers are in use and which latest dockers are available
# Usage: python3 docker_usage_sum.py
# Output: dockers.in_use.tsv
# Note: This script is not perfect. It will not be able to detect dockers that are
#       imported from other wdl files. It will only detect dockers that are
#       explicitly defined in the wdl file.
#       The script assumes it is executed from the scripts/docker directory, and the
#       wdl files are in ../../wdl directory.

def main():
    current_dir = os.path.abspath(os.path.dirname(__file__))

    print("COLLECTING DOCKERS IN USE...")
    wdls_dir = os.path.abspath(os.path.join(current_dir, "../../wdl"))
    sum_tsv_file = os.path.join(current_dir, "dockers.in_use.tsv")

    if os.path.exists(sum_tsv_file):
        os.remove(sum_tsv_file)

    wdl_files = get_wdl_files(dir_to_wdls=wdls_dir)
    global_docker_info = []

    total_files = len(wdl_files)  # Used for Progression calculation

    for index, wdl_path in enumerate(wdl_files, start=1):

        wdl_name = wdl_path

        with open(wdl_path, "r") as file:
            content = file.read()
            pattern = re.compile(r'.*docker.*"')
            if pattern.search(content):
                matched_lines = []
                file.seek(0)
                lines = file.readlines()

                for line_number, line in enumerate(lines, start=1):
                    if pattern.search(line):
                        matched_lines.append((line_number, line.strip()))

                docker_info: list[str] = get_docker_info_from_string(
                    wdl_lines=matched_lines, wdl_path=wdl_name
                )

                sorted_info: list = sorted(docker_info, reverse=False)

                global_docker_info.append(sorted_info)

        # Progression
        # Calculate the percentage completion
        progress = (index + 1) / total_files * 100

        # Clear the previous line and print the progress
        print(f"Progress: {progress:.2f}%\r", end="")

    with open(sum_tsv_file, "a") as tsv_file:
        tsv_file.write(f"DOCKER_NAME\tUSED_TAG\tLATEST_TAG\tFILE_LINE\tWDL_PATH\n")
        for line in sorted(global_docker_info):
            tsv_file.write("\n".join(line) + "\n")

    print(f"DONE. PLEASE CHECKOUT TSV FILE: {sum_tsv_file}")


def get_wdl_files(dir_to_wdls: str) -> list:
    """
    Returns a list of wdl files
    @return:
    """
    wdl_files = []
    for root, _, files in os.walk(dir_to_wdls):
        for filename in files:
            if filename.endswith(".wdl"):
                wdl_path = os.path.join(root, filename)
                wdl_files.append(wdl_path)

    return wdl_files


def get_docker_info_from_string(wdl_lines: [tuple], wdl_path: str) -> list:
    """
    Returns a list of docker info
    @param wdl_path:
    @param wdl_lines: (line_number, line_content)
    @return:
    """
    docker_detail = []

    wdl_path_sum = wdl_path[wdl_path.find("/wdl/"):]

    for line_num, line_content in wdl_lines:
        docker_names = re.findall(r'docker.*"(\S*?)"', line_content)
        if docker_names:
            docker_name = docker_names[0]
            used_tag = os.path.basename(docker_name).split(":")[1]
            docker_path = docker_name.split(":")[0]
            latest_tag = get_latest_local_docker_tag(docker_path)
            latest_tag = get_latest_remote_docker_tag(
                docker_path) if latest_tag == "NA" else latest_tag
            docker_detail.append(
                f"{docker_path}\t{used_tag}\t{latest_tag}\t{line_num}\t{wdl_path_sum}")
        else:
            pass

    return docker_detail


def get_latest_remote_docker_tag(docker_path: str) -> str:
    """
    Returns the latest tag of a docker
    @param docker_path:
    @return:
    """
    if "gcr" in docker_path or "ghcr" in docker_path:
        latest_tag = get_latest_tag_from_gcr(docker_path)
        if latest_tag == "NA" or latest_tag == "None":
            latest_tag = get_gcr_tag_with_gcloud(docker_path)
    elif "quay.io" in docker_path:
        latest_tag = get_latest_tag_from_quay(docker_path)
    else:
        latest_tag = get_latest_tag_from_dockerhub(docker_path)
    return latest_tag


def get_latest_tag_from_dockerhub(docker_path: str) -> str:

    """
    Returns the latest tag of a docker from dockerhub using the dockerhub API
    @param docker_path:
    @return:
    """

    image_name = docker_path
    registry_url = f"https://registry.hub.docker.com/v2/repositories/{image_name}/tags/?page_size=1&ordering=last_updated"
    try:
        with urllib.request.urlopen(registry_url) as response:
            data = response.read().decode("utf-8")
            json_data = json.loads(data)
            tags = json_data.get("results")
            if tags:
                latest_tag = tags[0].get("name")
                return latest_tag
            else:
                return "NA"
    except urllib.error.HTTPError as e:
        # print(f"Error: {e.code} - {e.reason}")
        pass
    except urllib.error.URLError as e:
        # print(f"Error: Failed to reach the server - {e.reason}")
        pass


def get_latest_tag_from_gcr(docker_path: str) -> str:

    """
    Returns the latest tag of a docker from GCR using the Container Registry API
    @param docker_path:
    @return:
    """

    # Split the image string into project ID and image name
    parts = docker_path.split("/")
    gcr_repo = parts[0]
    project_id = parts[1]
    image_name = "/".join(parts[2:])
    # Construct the URL for retrieving tags
    registry_url = f"https://{gcr_repo}/v2/{project_id}/{image_name}/tags/list?page_size=1&ordering=last_updated"

    try:
        # Send the GET request to the Container Registry API
        with urllib.request.urlopen(registry_url) as response:
            data = response.read().decode("utf-8")
            json_data = json.loads(data)
            tags = json_data.get("tags")

            tags_str_removed = [item for item in tags if any(char.isdigit() for char in item)]
            if tags_str_removed:
                if tags_str_removed is not None:
                    latest_tag = max(tags_str_removed)
                    return latest_tag
            else:
                return "NA"
    except urllib.error.HTTPError as e:
        # print(f"Error: {e.code} - {e.reason}")
        pass
    except urllib.error.URLError as e:
        # print(f"Error: Failed to reach the server - {e.reason}")
        pass


def get_gcr_tag_with_gcloud(docker_path: str) -> str or None:

    """
    Returns the latest tag of a docker using gcloud
    @param docker_path:
    @return:
    """

    # Split the image string into project ID and image name

    if is_gcloud_installed():

        command = [
            "gcloud",
            "container",
            "images",
            "list-tags",
            docker_path,
            "--format=get(tags)",
            "--limit=1",
            "--sort-by=~timestamp.datetime",
            "--filter=tags:*",
        ]

        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode == 0:
            output = process.stdout.strip()
            if output:
                latest_tag = output.splitlines()[0]
                return latest_tag

        # Error handling
        error_message = process.stderr.strip() if process.stderr else process.stdout.strip()
        #print(f"Error: {error_message}")
        return None
    else:
        return None


def is_gcloud_installed() -> bool:
    """
    Checks if gcloud is installed
    @return:
    """

    command = ["gcloud", "--version"]

    try:
        subprocess.run(command, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        return False


def get_latest_tag_from_quay(docker_path: str) -> str:
    """
    Returns the latest tag of a docker from quay.io
    @param docker_path:
    @return:
    """
    # Split the image string into project ID and image name
    parts = docker_path.split("/")
    quayio_repo = parts[0]
    project_id = parts[1]
    image_name = "/".join(parts[2:])
    # Construct the URL for retrieving tags
    registry_url = f"https://{quayio_repo}/v2/{project_id}/{image_name}/tags/list"

    try:
        # Send the GET request to the Container Registry API
        with urllib.request.urlopen(registry_url) as response:
            data = response.read().decode("utf-8")
            json_data = json.loads(data)
            tags = json_data.get("tags")

            tags_str_removed = [item for item in tags if any(char.isdigit() for char in item)]
            if tags_str_removed:
                latest_tag = max(tags_str_removed)
                return latest_tag
            else:
                return "NA"
    except urllib.error.HTTPError as e:
        # print(f"Error: {e.code} - {e.reason}")
        pass
    except urllib.error.URLError as e:
        # print(f"Error: Failed to reach the server - {e.reason}")
        pass


def get_latest_local_docker_tag(docker_path: str) -> str:
    """
    Returns the latest tag of a docker
    @param docker_path:
    @return:
    """
    docker_name = os.path.basename(docker_path)
    docker_dir = "../docker"
    latest_tag = "NA"

    for docker_im_dir in os.listdir(docker_dir):
        if docker_im_dir == docker_name:
            docker_dir_path = os.path.join(docker_dir, docker_im_dir)
            for makefile in os.listdir(docker_dir_path):
                if not makefile.endswith("Makefile"):
                    continue

                with open(os.path.join(docker_dir_path, makefile)) as f:
                    for makefile_line in f:
                        if "VERSION =" in makefile_line:
                            latest_tag = makefile_line.split("=")[1].strip()

    return latest_tag


if __name__ == "__main__":
    main()
