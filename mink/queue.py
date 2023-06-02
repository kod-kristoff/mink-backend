"""Utilities related to a queue of Sparv jobs.

The job queue lives in the cache and also on the local file system (as backup).
"""

import json
from pathlib import Path

from flask import current_app as app
from flask import g

from mink import jobs


def init():
    """Init a queue from the filesystem if it has not been initialized already."""
    if not g.cache.get_queue_initialized():
        app.logger.info("Initializing queue")
        all_jobs = []  # Storage for all jobs, including done, aborted and errorneous
        queue_dir = Path(app.instance_path) / app.config.get("QUEUE_DIR")
        queue_dir.mkdir(exist_ok=True)
        # Load queue priorities
        priorities_file = queue_dir / Path(app.config.get("QUEUE_FILE"))
        if priorities_file.is_file():
            with priorities_file.open() as p:
                jsonstr = p.read()
                queue = json.loads(jsonstr) or []
        else:
            queue = []
        g.cache.set_queue_initialized(True)
        # Load jobs into memory, append to queue if necessary
        for f in sorted(queue_dir.iterdir(), key=lambda x: x.stat().st_mtime):
            if f == priorities_file:
                continue
            with f.open() as fobj:
                job = jobs.load_from_str(fobj.read())
                job.save()  # Update job in file system and add to cache
                all_jobs.append(job.corpus_id)
                app.logger.debug(f"Job in cache: '{g.cache.get_job(job.corpus_id)}'")
            # Queue job unless it is done, aborted or erroneous
            if job.corpus_id not in queue:
                if not job.status.is_done(job.current_process) and not job.status.is_inactive():
                    queue.append(job.corpus_id)
        g.cache.set_job_queue(queue)
        g.cache.set_all_jobs(all_jobs)
        app.logger.debug(f"Queue in cache: {g.cache.get_job_queue()}")
        app.logger.debug(f"All jobs in cache: {g.cache.get_all_jobs()}")


def add(job):
    """Add a job item to the queue."""
    queue = g.cache.get_job_queue()

    # Avoid starting multiple jobs for the same corpus simultaneously
    if job.corpus_id in queue and job.status.is_active():
        raise Exception("There is an unfinished job for this corpus!")

    # Unqueue if old job is queued since before
    if job.corpus_id in queue:
        queue.pop(queue.index(job.corpus_id))
    # Add job to queue and save priority
    queue.append(job.corpus_id)
    g.cache.set_job_queue(queue)
    save_priorities()
    # Save to all_jobs
    all_jobs = g.cache.get_all_jobs()
    all_jobs.append(job.corpus_id)
    g.cache.set_all_jobs(all_jobs)
    app.logger.debug(f"Queue in cache: {g.cache.get_job_queue()}")
    return job


def get():
    """Get the first job item from the queue."""
    queue = g.cache.get_job_queue()
    job = g.cache.get_job(queue[0])
    return job


def remove(job):
    """Remove job item from queue, e.g. when a job is aborted or a corpus is deleted."""
    queue = g.cache.get_job_queue()
    if job.corpus_id in queue:
        queue.pop(queue.index(job.corpus_id))
        g.cache.set_job_queue(queue)
        save_priorities()

    all_jobs = g.cache.get_all_jobs()
    if job.corpus_id in all_jobs:
        all_jobs.pop(all_jobs.index(job.corpus_id))
        g.cache.set_all_jobs(all_jobs)


def get_priority(job):
    """Get the queue priority of the job."""
    _, waiting_jobs = get_running_waiting()
    waiting_jobs = [j.corpus_id for j in waiting_jobs]
    try:
        return waiting_jobs.index(job.corpus_id) + 1
    except ValueError:
        return -1


def save_priorities():
    """Save queue order so it can be loaded from disk upon app restart."""
    queue_dir = Path(app.instance_path) / Path(app.config.get("QUEUE_DIR"))
    queue_dir.mkdir(exist_ok=True)
    queue = g.cache.get_job_queue()
    priorities_file = queue_dir / Path(app.config.get("QUEUE_FILE"))
    with priorities_file.open("w") as f:
        f.write(json.dumps(queue))


def get_running_waiting():
    """Get the running and waiting jobs from the queue."""
    running_jobs = []
    waiting_jobs = []

    queue = g.cache.get_job_queue()
    for j in queue:
        job = jobs.load_from_str(g.cache.get_job(j))
        if job.status.is_running():
            running_jobs.append(job)
        elif job.status.is_waiting():
            waiting_jobs.append(job)

    return running_jobs, waiting_jobs


def unqueue_inactive():
    """Unqueue jobs that are done, aborted or erroneous."""
    queue = g.cache.get_job_queue()
    old_jobs = []
    for j in queue:
        job = jobs.load_from_str(g.cache.get_job(j))
        if job.status.is_inactive():
            old_jobs.append(j)

    if old_jobs:
        for j in old_jobs:
            app.logger.info(f"Removing job {j}")
            queue.pop(queue.index(j))
        g.cache.set_job_queue(queue)
        save_priorities()


def get_jobs(corpora: list = None):
    """Get info for all jobs or (if specified) only for the corpora in 'corpora'."""
    loaded_jobs = []
    all_jobs = g.cache.get_all_jobs()
    for j in all_jobs:
        if corpora is not None and j not in corpora:
            continue
        job = jobs.load_from_str(g.cache.get_job(j))
        loaded_jobs.append(job)
    return loaded_jobs


def get_job_by_corpus_id(corpus_id):
    """Get a job object belonging to a corpus ID."""
    if corpus_id in g.cache.get_all_jobs():
        job = jobs.load_from_str(g.cache.get_job(corpus_id))
        return job
    return False
