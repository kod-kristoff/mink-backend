"""Functions related to storage on Sparv server."""

import os
import subprocess
from pathlib import Path

from dateutil.parser import parse
from flask import current_app as app

from minsb import utils
from minsb.sparv import utils as sparv_utils


def list_contents(_ui, directory, exclude_dirs=True):
    """List files in directory on Sparv server recursively."""
    objlist = []

    user, host = _get_login()
    p = subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", f"{user}@{host}",
                        f"cd /home/{user} && find {directory} -exec ls -lgGd --time-style=long-iso {{}} \\;"
                        f"-exec file --mime-type {{}} \\;"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to list contents of '{directory}': {p.stderr.decode()}")

    contents = p.stdout.decode()
    for line in contents.split("\n"):
        content_type = ""
        if line.startswith("./"):
            content_type = line.split(": ")[-1]
        else:
            permissions, _, size, date, time, obj_path = line.split()
            name = Path(obj_path).name
            mod_time = parse(f"{date} {time}").isoformat()
            if permissions.startswith("d"):
                if exclude_dirs:
                    continue
            objlist.append(
                {"name": name, "type": content_type,
                "last_modified": mod_time, "size": size, "path": obj_path})
    return objlist


def download_dir(_ui, remote_dir, local_dir, _corpus_id, _file_index):
    """Download remote_dir on Sparv server to local_dir by rsyncing."""
    if not _is_valid_path(remote_dir):
        raise Exception(f"You don't have permission to download '{remote_dir}'")

    if not local_dir.is_dir():
        raise Exception(f"'{local_dir}' is not a directory")

    user, host = _get_login()
    p = subprocess.run(["rsync", "--recursive", f"{local_dir}/", f"{user}@{host}:{remote_dir}"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to download '{remote_dir}': {p.stderr.decode()}")


def get_file_contents(_ui, filepath):
    """Get contents of file at 'filepath'."""
    user, host = _get_login()
    p = subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", f"{user}@{host}",
                        f"cd /home/{user} && cat {filepath}"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to retrieve contents for '{filepath}': {p.stderr.decode()}")

    return p.stdout.decode()


def upload_dir(_ui, remote_dir, local_dir, _corpus_id, _user, _file_index, delete=False):
    """Upload local dir to remote_dir on Sparv server by rsyncing.

    Args:
        remote_dir: Directory on Sparv to upload to.
        local_dir: Local directory to upload.
        delete: If set to True delete files that do not exist in local_dir.
    """
    if not _is_valid_path(remote_dir):
        raise Exception(f"You don't have permission to edit '{remote_dir}'")

    if not local_dir.is_dir():
        raise Exception(f"'{local_dir}' is not a directory")

    if delete:
        args = ["--recursive", "--delete", f"{local_dir}/"]
    else:
        args = ["--recursive", f"{local_dir}/"]

    _make_dir(remote_dir)
    user, host = _get_login()
    p = subprocess.run(["rsync"] + args + [f"{user}@{host}:{remote_dir}"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to upload to '{remote_dir}': {p.stderr.decode()}")


def create_file_index(contents, user):
    """Convert Nextcloud contents list to a file index with local paths and timestamps."""
    # TODO: Is this needed for anything?
    file_index = {}
    for f in contents:
        parts = f.get("path").split("/")
        user_dir = str(utils.get_corpora_dir(user))
        new_path = os.path.join(user_dir, *parts[2:])
        unix_timestamp = int(parse(f.get("last_modified")).astimezone().timestamp())
        file_index[new_path] = unix_timestamp
    return file_index


def remove_dir(_ui, dirpath):
    """Remove directory on 'path' from Sparv server."""
    user = app.config.get("SPARV_USER")
    host = app.config.get("SPARV_HOST")

    if not _is_valid_path(dirpath):
        raise Exception(f"You don't have permission to remove '{dirpath}'")

    p = subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", f"{user}@{host}",
                        f"cd /home/{user} && rm -r {dirpath}"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to remove corpus dir on Sparv server {p.stderr.decode()}")


def _get_login():
    user = app.config.get("SPARV_USER")
    host = app.config.get("SPARV_HOST")
    return user, host

def _is_valid_path(path):
    """Check that path points to a corpus dir (or a descendant) and not to e.g. the entire Sparv data dir."""
    # TODO
    return True


################################################################################
# Get paths on Sparv server
################################################################################

def get_corpora_dir(_ui, mkdir=False):
    """Get corpora directory."""
    corpora_dir = sparv_utils.get_corpora_dir()
    if mkdir:
        _make_dir(corpora_dir)
    return corpora_dir


def get_corpus_dir(_ui, corpus_id, mkdir=False):
    """Get dir for given corpus."""
    corpus_dir = sparv_utils.get_corpus_dir(corpus_id)
    if mkdir:
        _make_dir(corpus_dir)
    return corpus_dir


def get_export_dir(_ui, corpus_id, mkdir=False):
    """Get export dir for given corpus."""
    export_dir = sparv_utils.get_export_dir(corpus_id)
    if mkdir:
        _make_dir(export_dir)
    return export_dir


def get_work_dir(_ui, corpus_id, _mkdir):
    """Get sparv workdir for given corpus."""
    return sparv_utils.get_work_dir(corpus_id)


def get_source_dir(_ui, corpus_id, mkdir=False):
    """Get source dir for given corpus."""
    source_dir = sparv_utils.get_source_dir(corpus_id)
    if mkdir:
        _make_dir(source_dir)
    return source_dir


def get_config_file(_ui, corpus_id):
    """Get path to corpus config file."""
    return sparv_utils.get_config_file(corpus_id)


def _make_dir(dirpath):
    """Create directory on Sparv server."""
    user, host = _get_login()
    p = subprocess.run(["ssh", "-i", "~/.ssh/id_rsa", f"{user}@{host}",
                        f"cd /home/{user} && mkdir -p {dirpath}"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.stderr:
        raise Exception(f"Failed to create corpus dir on Sparv server! {p.stderr.decode()}")