"""Classes defining job statuses."""

import json
from enum import Enum
from typing import Optional


class Status(Enum):
    """Class for representing the status of a Sparv job."""

    none = "Process does not exist"
    waiting = "Waiting to be processed"
    running = "Process is running"
    done = "Process has finished"
    error = "An error occurred in the process"
    aborted = "Process was aborted by the user"


class ProcessName(Enum):
    sync2sparv = "sync2sparv"
    sync2storage = "sync2storage"
    sparv = "sparv"
    korp = "korp"


class JobStatuses(dict):
    """Class for representing the statuses of the different job processes."""

    def __init__(self, status: Optional[dict] = None):
        """Init the status for the different processes, default to none."""
        # Override the old status format
        if not isinstance(status, dict):
            status = {}

        mapping = [(pn.name, getattr(Status, status.get(pn.name, ""), Status.none)) for pn in ProcessName]
        dict.__init__(self, mapping)

    def __str__(self):
        return json.dumps(self.dump())

    def dump(self):
        return {k: v.name for k, v in self.items()}

    def is_active(self, process_name=None):
        """Check if status is active."""
        if process_name:
            return self.get(process_name) in [Status.waiting, Status.running]
        return any(status in [Status.waiting, Status.running] for status in self.values())

    def is_inactive(self):
        """Check if status is inactive."""
        return all(status in [Status.none, Status.done, Status.error, Status.aborted]
                   for status in self.values())

    def is_syncing(self):
        """Check if status is syncing."""
        return self.get(ProcessName.sync2sparv) == Status.running or self.get(
            ProcessName.sync2storage) == Status.running

    def is_none(self, process_name=None):
        """Check if status is none."""
        if process_name:
            return self.get(process_name) == Status.none
        return all(status == Status.none for status in self.values())

    def is_waiting(self, process_name=None):
        """Check if status is waiting."""
        if process_name:
            return self.get(process_name) == Status.waiting
        return any(status == Status.waiting for status in self.values())

    def is_running(self, process_name=None):
        """Check if status is running."""
        if process_name:
            return self.get(process_name) == Status.running
        return any(status == Status.running for status in self.values())

    def is_done(self, process_name):
        """Check if status is done processing."""
        if process_name is None:
            return False
        return self.get(process_name) == Status.done

    def is_error(self, process_name):
        """Check if status is error."""
        if process_name is None:
            return False
        return self.get(process_name) == Status.error

    def is_aborted(self, process_name):
        """Check if status is error."""
        if process_name is None:
            return False
        return self.get(process_name) == Status.aborted

    def has_process_output(self, process_name):
        """Check if process is expected to have process output."""
        if process_name is None:
            return False
        if process_name not in [ProcessName.sync2sparv, ProcessName.sync2storage]:
            return self.get(process_name) in [Status.running, Status.done, Status.error]
        return False
