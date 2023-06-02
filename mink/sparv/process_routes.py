"""Routes related to Sparv."""

import time

from flask import Blueprint
from flask import current_app as app
from flask import request, session

from mink import exceptions, jobs, queue, utils
from mink.sb_auth import login
from mink.sparv import storage
from mink.status import JobStatuses, ProcessName, Status

bp = Blueprint("sparv", __name__)


@bp.route("/run-sparv", methods=["PUT"])
@login.login()
def run_sparv(user_id: str, contact: str, corpus_id: str):
    """Run Sparv on given corpus."""
    # Parse requested exports
    sparv_exports = request.args.get("exports") or request.form.get("exports") or ""
    sparv_exports = [i.strip() for i in sparv_exports.split(",") if i] or app.config.get("SPARV_DEFAULT_EXPORTS")

    files = request.args.get("files") or request.form.get("files") or ""
    files = [i.strip() for i in files.split(",") if i]

    # Get list of available source files to be stored in the job info
    source_dir = str(storage.get_source_dir(corpus_id))
    try:
        source_files = storage.list_contents(source_dir)
    except Exception as e:
        return utils.response(f"Failed to list source files in '{corpus_id}'", err=True, info=str(e)), 500

    if not source_files:
        return utils.response(f"No source files found for '{corpus_id}'", err=True), 404

    # Check compatibility between source files and config
    try:
        config_file = str(storage.get_config_file(corpus_id))
        config_contents = storage.get_file_contents(config_file)
        if source_files:
            compatible, resp = utils.config_compatible(config_contents, source_files[0])
            if not compatible:
                return resp, 400
    except Exception as e:
        return utils.response(f"Failed to get config file for '{corpus_id}'", err=True, info=str(e)), 500

    # Get job, check for changes and remove exports if necessary
    try:
        old_job = jobs.get_job(corpus_id)
        _, _, deleted_sources, changed_config = storage.get_file_changes(corpus_id, old_job)
        if deleted_sources or changed_config:
            try:
                success, sparv_output = old_job.clean_export()
                assert success
            except Exception as e:
                return utils.response(f"Failed to remove export files from Sparv server for corpus '{corpus_id}'. "
                                    "Cannot run Sparv safely", err=True, info=str(e), sparv_message=sparv_output), 500
    except exceptions.JobNotFound:
        pass
    except exceptions.CouldNotListSources as e:
        return utils.response(f"Failed to list source files in '{corpus_id}'", err=True, info=str(e)), 500

    job = jobs.get_job(corpus_id, user_id=user_id, contact=contact, sparv_exports=sparv_exports, files=files,
                       available_files=source_files)
    # Queue job
    job.reset_time()
    try:
        job = queue.add(job)
    except Exception as e:
        return utils.response(f"Failed to queue job for '{corpus_id}'", err=True, info=str(e)), 500

    # Check that all required files are present
    job.check_requirements()

    if storage.local:
        job.set_status(Status.waiting, ProcessName.sparv)
    else:
        # Sync files
        try:
            job.sync_to_sparv()
        except Exception as e:
            return utils.response(f"Failed to start job for '{corpus_id}'", err=True, info=str(e)), 500

    # Wait a few seconds to check whether anything terminated early
    time.sleep(3)
    return make_status_response(job)


@bp.route("/advance-queue", methods=["PUT"])
@utils.gatekeeper
def advance_queue():
    """Check the job queue and attempt to advance it.

    1. Unqueue jobs that are done, aborted or erroneous
    2. For running jobs, check if process is still running
    3. Run the next job in the queue if there are fewer running jobs than allowed

    For internal use only!
    """
    # Unqueue jobs that are done, aborted or erroneous
    queue.unqueue_inactive()

    # For running jobs, check if process is still running
    running_jobs, waiting_jobs = queue.get_running_waiting()
    app.logger.debug(f"Running jobs: {len(running_jobs)}  Waiting jobs: {len(waiting_jobs)}")
    for job in running_jobs:
        try:
            if not job.process_running():
                running_jobs.remove(job)
        except Exception as e:
            app.logger.error(f"Failed to check if process is running for '{job.corpus_id}' {str(e)}")

    # If there are fewer running jobs than allowed, start the next one in the queue
    while waiting_jobs and len(running_jobs) < app.config.get("SPARV_WORKERS", 1):
        job = waiting_jobs.pop(0)
        try:
            if job.status.is_waiting():
                if job.current_process == ProcessName.sparv.name:
                    job.run_sparv()
                    app.logger.info(f"Started annotation process for '{job.corpus_id}'")
                elif job.current_process == ProcessName.korp.name:
                    job.install_korp()
                    app.logger.info(f"Started installation process for '{job.corpus_id}'")
            running_jobs.append(job)
        except Exception as e:
            app.logger.error(f"Failed to run Sparv on '{job.corpus_id}' {str(e)}")

    return utils.response("Queue advancing completed")


@bp.route("/check-status", methods=["GET"])
@login.login(require_corpus_id=False)
def check_status(corpora: list):
    """Check the annotation status for all jobs belonging to a user or a given corpus."""
    corpus_id = request.args.get("corpus_id") or request.form.get("corpus_id")
    if corpus_id:
        try:
            # Check if corpus exists
            if corpus_id not in corpora:
                return utils.response(f"Corpus '{corpus_id}' does not exist or you do not have access to it",
                                      err=True), 404
            job = queue.get_job_by_corpus_id(corpus_id)
            if not job:
                return utils.response(f"There is no active job for '{corpus_id}'", job_status=JobStatuses())

            return make_status_response(job, admin=session.get("admin_mode", False))
        except Exception as e:
            return utils.response(f"Failed to get job status for '{corpus_id}'", err=True, info=str(e)), 500

    try:
        # Get all job statuses for this user's corpora
        job_list = []
        all_jobs = queue.get_jobs(corpora)
        for job in all_jobs:
            resp = make_status_response(job, admin=session.get("admin_mode", False))
            if isinstance(resp, tuple):
                resp = resp[0]
            job_status = {"corpus_id": job.corpus_id}
            job_status.update(resp.get_json())
            job_status.pop("status")
            job_list.append(job_status)
        return utils.response("Listing jobs", jobs=job_list)
    except Exception as e:
        return utils.response("Failed to get job statuses", err=True, info=str(e)), 500


@bp.route("/abort-job", methods=["POST"])
@login.login()
def abort_job(corpus_id: str):
    """Try to abort a running job."""
    job = jobs.get_job(corpus_id)
    # Syncing
    if job.status.is_syncing():
        return utils.response(f"Cannot abort job while syncing files", job_status=job.status), 503
    # Waiting
    if job.status.is_waiting():
        try:
            queue.remove(job)
            job.set_status(Status.aborted)
            return utils.response(f"Successfully unqueued job for '{corpus_id}'", job_status=job.status)
        except Exception as e:
            return utils.response(f"Failed to unqueue job for '{corpus_id}'", err=True, info=str(e)), 500
    # No running job
    if not job.status.is_running():
        return utils.response(f"No running job found for '{corpus_id}'")
    # Running job, try to abort
    try:
        job.abort_sparv()
    except exceptions.ProcessNotRunning:
        return utils.response(f"No running job found for '{corpus_id}'")
    except Exception as e:
        return utils.response(f"Failed to abort job for '{corpus_id}'", err=True, info=str(e)), 500
    return utils.response(f"Successfully aborted running job for '{corpus_id}'", job_status=job.status)


@bp.route("/clear-annotations", methods=["DELETE"])
@login.login()
def clear_annotations(corpus_id: str):
    """Remove annotation files from Sparv server."""
    # Check if there is an active job
    job = jobs.get_job(corpus_id)
    if job.status.is_running():
        return utils.response("Cannot clear annotations while a job is running", err=True), 503

    try:
        sparv_output = job.clean()
        return utils.response(f"Annotations for '{corpus_id}' successfully removed", sparv_output=sparv_output)
    except Exception as e:
        return utils.response("Failed to clear annotations", err=True, info=str(e)), 500


@bp.route("/install-corpus", methods=["PUT"])
@login.login()
def install_corpus(user_id: str, contact: str, corpus_id: str):
    """Install a corpus on Korp with Sparv."""
    # Get info about whether the corpus should be scrambled in Korp. Default to not scrambling.
    scramble = request.args.get("scramble", "") or request.form.get("scramble", "")
    scramble = scramble.lower() == "true"

    # Get job, check for changes and remove exports if necessary
    try:
        old_job = jobs.get_job(corpus_id)
        _, _, deleted_sources, changed_config = storage.get_file_changes(corpus_id, old_job)
        if deleted_sources or changed_config:
            try:
                success, sparv_output = old_job.clean_export()
                assert success
            except Exception as e:
                return utils.response(f"Failed to remove export files from Sparv server for corpus '{corpus_id}'. "
                                      "Cannot run Sparv safely", err=True, info=str(e), sparv_message=sparv_output), 500
    except exceptions.JobNotFound:
        pass
    except exceptions.CouldNotListSources as e:
        return utils.response(f"Failed to list source files in '{corpus_id}'", err=True, info=str(e)), 500

    # Queue job
    job = jobs.get_job(corpus_id, user_id=user_id, contact=contact, install_scrambled=scramble)
    job.reset_time()
    job.set_install_scrambled(scramble)
    try:
        job = queue.add(job)
    except Exception as e:
        return utils.response(f"Failed to queue job for '{corpus_id}'", err=True, info=str(e)), 500

    job.set_status(Status.waiting, ProcessName.korp)

    # Wait a few seconds to check whether anything terminated early
    time.sleep(3)
    return make_status_response(job)


def make_status_response(job, admin=False):
    """Check the annotation status for a given corpus and return response."""
    status = job.status
    job_attrs = {"job_status": status.dump(), "sparv_exports": job.sparv_exports,
                 "available_files": job.available_files, "installed_korp": job.installed_korp}
    warnings, errors, misc_output = job.get_output()

    job_attrs["files"] = job.files or ""
    if job.install_scrambled is not None:
        job_attrs["install_scrambled"] = job.install_scrambled
    if job.current_process is not None:
        job_attrs["current_process"] = job.current_process
    job_attrs["seconds_taken"] = job.seconds_taken or ""
    job_attrs["last_run_started"] = job.started or ""
    job_attrs["last_run_ended"] = job.done or ""
    job_attrs["progress"] = job.progress or ""

    if admin:
        job_attrs["user"] = job.contact

    if status.is_none():
        return utils.response(f"There is no active job for '{job.corpus_id}'", job_status=status.dump())

    if status.is_syncing():
        return utils.response("Files are being synced", **job_attrs)

    if status.is_waiting():
        return utils.response("Job has been queued", **job_attrs, priority=queue.get_priority(job))

    if status.is_aborted(job.current_process):
        return utils.response("Job was aborted by the user", **job_attrs)

    if status.is_running():
        return utils.response("Job is running", warnings=warnings, errors=errors, sparv_output=misc_output,
                              **job_attrs)

    # If done annotating, retrieve exports from Sparv
    if status.is_done(ProcessName.sparv) and not admin:
        try:
            job.sync_results()
        except Exception as e:
            return utils.response("Sparv was run successfully but exports failed to upload to the storage server",
                                  info=str(e))
        return utils.response("Sparv was run successfully! Starting to sync results", warnings=warnings, errors=errors,
                              sparv_output=misc_output, **job_attrs)

    if status.is_done(job.current_process):
        return utils.response("Job was completed successfully!", warnings=warnings, errors=errors,
                              sparv_output=misc_output, **job_attrs)

    if status.is_error(job.current_process):
        app.logger.error(f"An error occurred during processing, warnings: {warnings}, errors: {errors}, "
                         f"sparv_output: {misc_output}, job_attrs: {job_attrs}")
        return utils.response("An error occurred during processing", warnings=warnings, errors=errors,
                              sparv_output=misc_output, **job_attrs)

    return utils.response("Cannot handle this Job status yet", warnings=warnings, errors=errors,
                          sparv_output=misc_output, **job_attrs), 501


@bp.route("/sparv-languages", methods=["GET"])
def sparv_languages():
    """List languages available in Sparv."""
    try:
        job = jobs.DefaultJob()
        languages = job.list_languages()
    except Exception as e:
        return utils.response("Failed to retrieve languages listing", err=True, info=str(e)), 500
    return utils.response("Listing languages available in Sparv", languages=languages)


@bp.route("/sparv-exports", methods=["GET"])
def sparv_exports():
    """List available Sparv exports for current language (default: swe)."""
    language = request.args.get("language") or request.form.get("language") or "swe"
    try:
        job = jobs.DefaultJob(language=language)
        exports = job.list_exports()
    except Exception as e:
        return utils.response("Failed to retrieve exports listing", err=True, info=str(e)), 500
    return utils.response("Listing exports available in Sparv", language=language, exports=exports)
