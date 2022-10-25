"""Utilities related to Sparv jobs."""

import datetime
import json
import re
import shlex
import subprocess
from enum import IntEnum
from itertools import count
from pathlib import Path

import dateutil
from flask import current_app as app
from flask import g

from mink import exceptions, queue, utils
from mink.sparv import storage
from mink.sparv import utils as sparv_utils

_status_count = count(0)


class Status(IntEnum):
    """Class for representing the status of a Sparv job.

    Inspired from:
    https://stackoverflow.com/questions/64038885/adding-attributes-and-starting-value-to-python-enum-intenum
    """

    none = "Job does not exist"
    syncing_corpus = "Syncing from the storage server to Sparv server"
    waiting = "Waiting to be run with Sparv"
    annotating = "Sparv annotation process is running"
    done_annotating = "Annotation process has finished"
    syncing_results = "Syncing results from Sparv to the storage server"
    done_syncing = "Results have been synced to storage server"
    waiting_install = "Waiting to be installed"
    installing = "Corpus is being installed"
    done_installing = "Corpus is done installing"
    error = "An error occurred"
    aborted = "Aborted by the user"

    def __new__(cls, desc):
        value = next(_status_count)
        member = int.__new__(cls, value)
        member._value_ = value
        member.desc = desc
        return member

    @classmethod
    def is_active(cls, status):
        """Check if status is active."""
        return status in [cls.syncing_corpus, cls.waiting, cls.annotating, cls.waiting_install, cls.installing]

    @classmethod
    def is_inactive(cls, status):
        """Check if status is inactive."""
        return status in [cls.done_syncing, cls.done_installing, cls.error, cls.aborted]

    @classmethod
    def is_syncing(cls, status):
        """Check if status is syncing."""
        return status in [cls.syncing_corpus, cls.syncing_results]

    @classmethod
    def is_waiting(cls, status):
        """Check if status is waiting."""
        return status in [cls.waiting, cls.waiting_install]

    @classmethod
    def is_running(cls, status):
        """Check if status is running."""
        return status in [cls.annotating, cls.installing]

    @classmethod
    def is_done_processing(cls, status):
        """Check if status is running."""
        return status in [cls.done_annotating, cls.done_syncing, cls.done_installing]

    @classmethod
    def has_process_output(cls, status):
        """Check if status is expected to have process output."""
        return status in [cls.annotating, cls.done_annotating, cls.syncing_results, cls.done_syncing, cls.installing,
                          cls.done_installing, cls.error]


class Job():
    """A job item holding information about a Sparv job."""

    def __init__(self, corpus_id, user_id=None, contact=None, status=Status.none, pid=None, started=None, done=None,
                 sparv_exports=None, files=None, available_files=None, install_scrambled=None, installed_korp=False,
                 latest_seconds_taken=0, **_obsolete):
        # **_obsolete is needed to catch invalid arguments from outdated job items in the queue (this avoids crashes)
        self.corpus_id = corpus_id
        self.contact = contact
        self.user_id = user_id
        self.status = status
        self.pid = pid
        self.started = started
        self.done = done
        self.sparv_done = None
        self.sparv_exports = sparv_exports or []
        self.files = files or []
        self.available_files = available_files or []
        self.install_scrambled = install_scrambled
        self.installed_korp = installed_korp
        self.latest_seconds_taken = latest_seconds_taken
        self.progress_output = "0%"

        self.sparv_user = app.config.get("SPARV_USER")
        self.sparv_server = app.config.get("SPARV_HOST")
        self.nohupfile = app.config.get("SPARV_NOHUP_FILE")
        self.runscript = app.config.get("SPARV_TMP_RUN_SCRIPT")
        self.remote_corpus_dir = str(sparv_utils.get_corpus_dir(corpus_id))

    def __str__(self):
        return json.dumps({"user_id": self.user_id, "contact": self.contact, "corpus_id": self.corpus_id,
                           "status": self.status.name, "pid": self.pid, "started": self.started, "done": self.done,
                           "sparv_exports": self.sparv_exports, "files": self.files, "available_files":
                           self.available_files, "install_scrambled": self.install_scrambled, "installed_korp":
                           self.installed_korp, "latest_seconds_taken": self.latest_seconds_taken})

    def save(self):
        """Write a job item to the cache and filesystem."""
        dump = str(self)
        # Save in cache
        g.cache.set_job(self.corpus_id, dump)
        # Save backup to file system queue
        queue_dir = Path(app.instance_path) / Path(app.config.get("QUEUE_DIR"))
        queue_dir.mkdir(exist_ok=True)
        backup_file = queue_dir / Path(self.corpus_id)
        with backup_file.open("w") as f:
            f.write(dump)

    def remove(self, abort=False):
        """Remove a job item from the cache and file system."""
        if self.status == Status.annotating:
            if abort:
                try:
                    self.abort_sparv()
                except exceptions.ProcessNotRunning:
                    pass
                except Exception as e:
                    raise e
            else:
                raise exceptions.JobError("Job cannot be removed due to a running Sparv process!")

        # Remove from cache
        try:
            g.cache.remove_job(self.corpus_id)
        except Exception as e:
            app.logger.error(f"Failed to delete job ID from cache client: {e}")
        # Remove backup from file system
        queue_dir = Path(app.instance_path) / Path(app.config.get("QUEUE_DIR"))
        filename = queue_dir / Path(self.corpus_id)
        filename.unlink(missing_ok=True)

    def set_status(self, status):
        """Change the status of a job."""
        if self.status != status:
            self.status = status
            self.save()

    def set_pid(self, pid):
        """Set pid of job and save."""
        self.pid = pid
        self.save()

    def set_install_scrambled(self, scramble):
        """Set status of 'install_scrambled' and save."""
        self.install_scrambled = scramble
        self.save()

    def set_latest_seconds_taken(self, seconds_taken):
        """Set 'latest_seconds_taken' and save."""
        if self.latest_seconds_taken != seconds_taken:
            self.latest_seconds_taken = seconds_taken
            self.save()

    def reset_time(self):
        """Reset the processing time for a job (e.g. when starting a new one)."""
        self.latest_seconds_taken = 0
        self.started = None
        self.done = None
        self.sparv_done = None
        self.save()

    def check_requirements(self):
        """Check if required corpus contents are present."""
        remote_corpus_dir = str(storage.get_corpus_dir(self.corpus_id))
        corpus_contents = storage.list_contents(remote_corpus_dir, exclude_dirs=False)
        if not app.config.get("SPARV_CORPUS_CONFIG") in [i.get("name") for i in corpus_contents]:
            self.set_status(Status.error)
            raise Exception(f"No config file provided for '{self.corpus_id}'!")
        if not len([i for i in corpus_contents if i.get("path").startswith(app.config.get("SPARV_SOURCE_DIR"))]):
            self.set_status(Status.error)
            raise Exception(f"No input files provided for '{self.corpus_id}'!")

    def sync_to_sparv(self):
        """Sync corpus files from storage server to the Sparv server."""
        self.set_status(Status.syncing_corpus)

        # Get relevant directories
        remote_corpus_dir = str(storage.get_corpus_dir(self.corpus_id))
        local_user_dir = utils.get_corpora_dir(mkdir=True)

        # Create user and corpus dir on Sparv server
        p = utils.ssh_run(f"mkdir -p {shlex.quote(self.remote_corpus_dir)} && "
                          f"rm -f {shlex.quote(self.nohupfile)} {shlex.quote(self.runscript)}")
        if p.stderr:
            self.set_status(Status.error)
            raise Exception(f"Failed to create corpus dir on Sparv server! {p.stderr.decode()}")

        # Download from storage server to local tmp dir
        # TODO: do this async?
        try:
            storage.download_dir(remote_corpus_dir, local_user_dir, self.corpus_id)
        except Exception as e:
            self.set_status(Status.error)
            raise Exception(f"Failed to download corpus '{self.corpus_id}' from the storage server! {e}")

        # Sync corpus config to Sparv server
        p = subprocess.run(["rsync", "-av", utils.get_config_file(self.corpus_id),
                            f"{self.sparv_user}@{self.sparv_server}:~/{self.remote_corpus_dir}/"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.stderr:
            self.set_status(Status.error)
            raise Exception(f"Failed to copy corpus config file to Sparv server! {p.stderr.decode()}")

        # Sync corpus files to Sparv server
        # TODO: do this async!
        local_source_dir = utils.get_source_dir(self.corpus_id)
        p = subprocess.run(["rsync", "-av", "--delete", local_source_dir,
                            f"{self.sparv_user}@{self.sparv_server}:~/{self.remote_corpus_dir}/"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.stderr:
            self.set_status(Status.error)
            raise Exception(f"Failed to copy corpus files to Sparv server! {p.stderr.decode()}")

        self.set_status(Status.waiting)

    def run_sparv(self):
        """Start a Sparv annotation process."""
        sparv_env = app.config.get("SPARV_ENVIRON")
        sparv_command = f"{app.config.get('SPARV_COMMAND')} {app.config.get('SPARV_RUN')} {' '.join(self.sparv_exports)}"
        if self.files:
            sparv_command += f" --file {' '.join(shlex.quote(f) for f in self.files)}"
        script_content = f"{sparv_env} nohup time -p {sparv_command} >{self.nohupfile} 2>&1 &\necho $!"
        self.started = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && "
                          f"echo {shlex.quote(script_content)} > {shlex.quote(self.runscript)} && "
                          f"chmod +x {shlex.quote(self.runscript)} && ./{shlex.quote(self.runscript)}")

        if p.returncode != 0:
            stderr = p.stderr.decode() if p.stderr else ""
            self.reset_time()
            self.set_status(Status.error)
            raise exceptions.JobError(f"Failed to run Sparv! {stderr}")

        # Get pid from Sparv process and store job info
        self.set_pid(int(p.stdout.decode()))
        self.set_status(Status.annotating)

    def install_korp(self):
        """Install a corpus on Korp."""
        sparv_installs = app.config.get("SPARV_DEFAULT_INSTALLS")
        if self.install_scrambled:
            sparv_installs.append("cwb:install_corpus_scrambled")
        else:
            sparv_installs.extend(["cwb:install_corpus"])

        sparv_command = f"{app.config.get('SPARV_COMMAND')} {app.config.get('SPARV_INSTALL')} {' '.join(sparv_installs)}"
        sparv_env = app.config.get("SPARV_ENVIRON")

        self.started = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && "
                          f"echo '{sparv_env} nohup time -p {sparv_command} >{self.nohupfile} 2>&1 &\necho $!' "
                          f"> {shlex.quote(self.runscript)}"
                          f" && chmod +x {shlex.quote(self.runscript)} && ./{shlex.quote(self.runscript)}")

        if p.returncode != 0:
            stderr = p.stderr.decode() if p.stderr else ""
            self.reset_time()
            self.set_status(Status.error)
            raise exceptions.JobError(f"Failed to install corpus with Sparv. {stderr}")

        # Get pid from Sparv process and store job info
        self.installed_korp = True
        self.set_pid(int(p.stdout.decode()))
        self.set_status(Status.installing)

    def abort_sparv(self):
        """Abort running Sparv process."""
        if Status.is_waiting(self.status):
            queue.remove(self)
            self.set_status(Status.aborted)
            return
        if not Status.is_running(self.status):
            raise exceptions.ProcessNotRunning("Failed to abort job because Sparv was not running!")
        if not self.pid:
            raise exceptions.ProcessNotFound("Failed to abort job because no process ID was found!")

        p = utils.ssh_run(f"kill -SIGTERM {self.pid}")
        if p.returncode == 0:
            self.set_pid(None)
            self.set_status(Status.aborted)
        else:
            stderr = p.stderr.decode()
            # Ignore 'no such process' error
            if stderr.endswith("Processen finns inte\n") or stderr.endswith("No such process\n"):
                self.set_pid(None)
                self.set_status(Status.aborted)
            else:
                raise exceptions.JobError(f"Failed to abort job! Error: '{stderr}'")

    def process_running(self):
        """Check if process with this job's pid is still running on Sparv server."""
        if self.pid:
            p = utils.ssh_run(f"kill -0 {self.pid}")
            # Process is running, do nothing
            if p.returncode == 0:
                return True
            # Process not running anymore
            app.logger.debug(f"stderr: '{p.stderr.decode()}'")
            self.set_pid(None)

        _warnings, errors, misc = self.get_output()
        if (self.progress_output == "100%"):
            if self.status == Status.annotating:
                if storage.local:
                    self.set_status(Status.done_syncing)
                else:
                    self.set_status(Status.done_annotating)
            elif self.status == Status.installing:
                self.set_status(Status.done_installing)
        else:
            if errors:
                app.logger.debug(f"Error in Sparv: {errors}")
            if misc:
                app.logger.debug(f"Sparv output: {misc}")
            self.set_status(Status.error)
        return False

    def get_output(self):
        """Check latest Sparv output of this job by reading the nohup file."""
        if not Status.has_process_output(self.status):
            return "", "", ""

        nohupfile = app.config.get("SPARV_NOHUP_FILE")
        remote_corpus_dir = str(sparv_utils.get_corpus_dir(self.corpus_id))

        p = utils.ssh_run(f"cd {shlex.quote(remote_corpus_dir)} && cat {shlex.quote(nohupfile)}")

        stdout = p.stdout.decode().strip() if p.stdout else ""
        progress = warnings = errors = misc = ""
        if stdout:
            warnings = []
            errors = []
            misc = []
            latest_msg = misc
            for line in stdout.split("\n"):
                matchobj = re.match(r"(?:\d\d:\d\d:\d\d|\s{8}) ([A-Z]+)\s+(.+)$", line)
                if matchobj:
                    msg = matchobj.group(2).strip()
                    if matchobj.group(1) == "PROGRESS":
                        progress = msg
                    elif matchobj.group(1) == "WARNING":
                        warnings.append(matchobj.group(1) + " " + msg)
                        latest_msg = warnings
                    elif matchobj.group(1) == "ERROR":
                        errors.append(matchobj.group(1) + " " + msg)
                        latest_msg = errors
                # Catch line continuations
                elif re.match(r"\s{8,}.+", line):
                    latest_msg.append(line.strip())
                # Catch "real" time output
                elif re.match(r"real \d.+", line):
                    real_seconds = float(line[5:].strip())
                    self.sparv_done = (dateutil.parser.isoparse(self.started) +
                                       datetime.timedelta(seconds=real_seconds)).isoformat()
                # Ignore "user" and "sys" time output
                elif re.match(r"user|sys \d.+", line):
                    pass
                else:
                    if line.strip():
                        misc.append(line.strip())
            warnings = "\n".join(warnings)
            errors = "\n".join(errors)
            misc = "\n".join(misc)
            if misc.startswith("Nothing to be done."):
                progress = "100%"
            self.progress_output = progress

        return warnings, errors, misc

    @property
    def seconds_taken(self):
        """Calculate the time it took to process the corpus until it finished, aborted or until now.
        When a Sparv job is finished it reads the time Sparv took and compensates for extra time the backend
        may take.
        """
        if self.started == None or Status.is_waiting(self.status):
            seconds_taken = 0
        elif Status.is_running(self.status):
            now = datetime.datetime.now(datetime.timezone.utc)
            delta = now - dateutil.parser.isoparse(self.started)
            seconds_taken = max(self.latest_seconds_taken, delta.total_seconds())
        elif self.sparv_done:
            delta = dateutil.parser.isoparse(self.sparv_done) - dateutil.parser.isoparse(self.started)
            seconds_taken = max(self.latest_seconds_taken, delta.total_seconds())
            self.done = (dateutil.parser.isoparse(self.started) + datetime.timedelta(seconds=seconds_taken)).isoformat()
        else:
            # TODO: This should never happen. Can probably be removed.
            app.logger.debug(f"Something went wrong while calculating time taken. Job status: {self.status}")
            seconds_taken = 0

        self.set_latest_seconds_taken(seconds_taken)
        return seconds_taken


    @property
    def progress(self):
        """Get the Sparv progesss but don't report 100% before the job status has been changed to done."""
        if Status.has_process_output(self.status):
            if self.progress_output == "100%" and not Status.is_done_processing(self.status):
                return "99%"
            else:
                return self.progress_output
        elif Status.is_active(self.status):
            return "0%"
        else:
            return None


    def sync_results(self):
        """Sync exports from Sparv server to the storage server."""
        remote_corpus_dir = str(storage.get_corpus_dir(self.corpus_id))
        local_corpus_dir = str(utils.get_corpus_dir(self.corpus_id, mkdir=True))

        # Get exports from Sparv
        remote_export_dir = sparv_utils.get_export_dir(self.corpus_id)
        p = subprocess.run(["rsync", "-av", f"{self.sparv_user}@{self.sparv_server}:~/{remote_export_dir}",
                            local_corpus_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.stderr:
            self.set_status(Status.error)
            return utils.response("Failed to retrieve Sparv exports", err=True, info=p.stderr.decode()), 500

        # Get plain text sources from Sparv
        remote_work_dir = sparv_utils.get_work_dir(self.corpus_id)
        p = subprocess.run(["rsync", "-av", "--include=@text", "--include=*/", "--exclude=*", "--prune-empty-dirs",
                            f"{self.sparv_user}@{self.sparv_server}:~/{remote_work_dir}",
                            local_corpus_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Transfer exports to the storage server
        local_export_dir = utils.get_export_dir(self.corpus_id)
        try:
            storage.upload_dir(remote_corpus_dir, local_export_dir, self.corpus_id)
        except Exception as e:
            self.set_status(Status.error)
            raise Exception(f"Failed to upload exports to the storage server! {e}")

        # Transfer plain text sources to the storage server
        local_work_dir = utils.get_work_dir(self.corpus_id)
        try:
            app.logger.warning(local_work_dir)
            storage.upload_dir(remote_corpus_dir, local_work_dir, self.corpus_id)
        except Exception as e:
            self.set_status(Status.error)
            app.logger.warning(e)
            raise Exception(f"Failed to upload plain text sources to the storage server! {e}")

        self.set_status(Status.done_syncing)

    def remove_from_sparv(self):
        """Remove corpus dir from the Sparv server and abort running job if necessary."""
        try:
            self.abort_sparv()
        except exceptions.ProcessNotRunning:
            pass
        except Exception as e:
            raise e

        p = utils.ssh_run(f"rm -rf {shlex.quote(self.remote_corpus_dir)}")
        if p.stderr:
            app.logger.error(f"Failed to remove corpus dir '{self.remote_corpus_dir}'!")

    def clean(self):
        """Remove annotation and export files from Sparv server by running 'sparv clean --all'."""
        sparv_env = app.config.get("SPARV_ENVIRON")
        sparv_command = app.config.get("SPARV_COMMAND") + " clean --all"
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && "
                          f"rm -f {shlex.quote(self.nohupfile)} {shlex.quote(self.runscript)} && "
                          f"{sparv_env} {sparv_command}")

        if p.stderr:
            raise Exception(p.stderr.decode())

        sparv_output = p.stdout.decode() if p.stdout else ""
        sparv_output = ", ".join([line for line in sparv_output.split("\n") if line])
        return sparv_output

    def clean_export(self):
        """Remove export files from Sparv server by running 'sparv clean --export'."""
        sparv_env = app.config.get("SPARV_ENVIRON")
        sparv_command = app.config.get("SPARV_COMMAND") + " clean --export"
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && {sparv_env} {sparv_command}")
        if p.stderr:
            raise Exception(p.stderr.decode())

        sparv_output = p.stdout.decode() if p.stdout else ""
        sparv_output = ", ".join([line for line in sparv_output.split("\n") if line])
        return sparv_output


def get_job(corpus_id, user_id=None, contact=None, sparv_exports=None, files=None, available_files=None,
            install_scrambled=None):
    """Get an existing job from the cache or create a new one."""
    if g.cache.get_job(corpus_id) is not None:
        return load_from_str(g.cache.get_job(corpus_id), user_id=user_id, contact=contact, sparv_exports=sparv_exports,
                             files=files, available_files=available_files, install_scrambled=install_scrambled)
    return Job(corpus_id, user_id, contact, sparv_exports=sparv_exports, files=files, available_files=available_files,
               install_scrambled=install_scrambled)


def load_from_str(jsonstr, user_id=None, contact=None, sparv_exports=None, files=None, available_files=None,
                  install_scrambled=None):
    """Load a job object from a json string."""
    job_info = json.loads(jsonstr)
    job_info["status"] = getattr(Status, job_info.get("status"))
    if user_id is not None:
        job_info["user_id"] = user_id
    if contact is not None:
        job_info["contact"] = contact
    if sparv_exports is not None:
        job_info["sparv_exports"] = sparv_exports
    if files is not None:
        job_info["files"] = files
    if available_files is not None:
        job_info["available_files"] = available_files
    if install_scrambled is not None:
        job_info["install_scrambled"] = install_scrambled
    return Job(**job_info)


class DefaultJob():
    """A default job item for running generic Sparv commands like `sparv run -l`.

    A default job is not part of the job queue
    """

    def __init__(self, language="swe"):
        self.lang = language

        self.sparv_user = app.config.get("SPARV_USER")
        self.sparv_server = app.config.get("SPARV_HOST")
        self.remote_corpus_dir = str(sparv_utils.get_corpus_dir(self.lang, default_dir=True))
        self.config_file = app.config.get("SPARV_CORPUS_CONFIG")

    def list_languages(self):
        """List the languages available in Sparv."""
        # Create and corpus dir with config file on Sparv server
        p = utils.ssh_run(f"mkdir -p {shlex.quote(self.remote_corpus_dir)} && "
                          f"echo 'metadata:\n  language: {self.lang}' > "
                          f"{shlex.quote(self.remote_corpus_dir + '/' + self.config_file)}")
        if p.stderr:
            raise Exception(f"Failed to list languages! {p.stderr.decode()}")

        sparv_env = app.config.get("SPARV_ENVIRON")
        sparv_command = f"{app.config.get('SPARV_COMMAND')} languages"
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && {sparv_env} {sparv_command}")

        if p.returncode != 0:
            stderr = p.stderr.decode() if p.stderr else ""
            raise exceptions.JobError(f"Failed to run Sparv! {stderr}")

        languages = []
        stdout = p.stdout.decode() if p.stdout else ""
        lines = [line.strip() for line in stdout.split("\n") if line.strip()][1:]
        for line in lines:
            if line.startswith("Supported language varieties"):
                break
            matchobj = re.match(r"(.+?)\s+(\S+)$", line)
            if matchobj:
                languages.append({"name": matchobj.group(1), "code": matchobj.group(2)})
        return languages

    def list_exports(self):
        """List the available exports for the current language."""
        # Create and corpus dir with config file on Sparv server
        p = utils.ssh_run(f"mkdir -p {shlex.quote(self.remote_corpus_dir)} && "
                          f"echo 'metadata:\n  language: {self.lang}' > "
                          f"{shlex.quote(self.remote_corpus_dir + '/' + self.config_file)}")
        if p.stderr:
            raise Exception(f"Failed to list exports! {p.stderr.decode()}")

        sparv_env = app.config.get("SPARV_ENVIRON")
        sparv_command = f"{app.config.get('SPARV_COMMAND')} run -l"
        p = utils.ssh_run(f"cd {shlex.quote(self.remote_corpus_dir)} && {sparv_env} {sparv_command}")

        if p.returncode != 0:
            stderr = p.stderr.decode() if p.stderr else ""
            raise exceptions.JobError(f"Failed to run Sparv! {stderr}")

        exports = []
        stdout = p.stdout.decode() if p.stdout else ""
        lines = [line for line in stdout.split("\n") if line.strip()][1:-1]
        for line in lines:
            if line.startswith("    "):
                exports[-1]["description"] += " " + line.strip()
            else:
                matchobj = re.match(r"(\S+)\s+(.+)$", line.strip())
                if matchobj:
                    if matchobj.group(1) not in ["Other", "Note:", "what", "'export.default'"]:
                        exports.append({"export": matchobj.group(1), "description": matchobj.group(2)})
        return exports
