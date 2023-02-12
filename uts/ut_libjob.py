from pyjobs.private import libjob
from unittest.mock import MagicMock

def test_libjob_one_job():
    pipeline = [MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_libjob_3_jobs():
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_libjob_1():
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    pipeline1 = [MagicMock(), MagicMock(), MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((0, update_id, pipeline))
    jp.process_pipeline((1, update_id, pipeline1))
    jp.wait_for_finish(0)
    for i in pipeline: i.assert_called_once()
    jp.wait_for_finish(1)
    for i in pipeline1: i.assert_called_once()

from pyjobs.private.libjob import JobsProcess
import pytest

def test_register_callback():
    jobs_process = JobsProcess()
    callback_called = False
    def callback(job_id, job_status):
        nonlocal callback_called
        callback_called = True
    jobs_process.register_callback(callback)
    jobs_process._call_callbacks(1, "STARTED")
    assert callback_called

def test_unregister_callback():
    jobs_process = JobsProcess()
    callback_called = False
    def callback(job_id, job_status):
        nonlocal callback_called
        callback_called = True
    jobs_process.register_callback(callback)
    jobs_process.unregister_callback(callback)
    jobs_process._call_callbacks(1, "STARTED")
    assert not callback_called

def test_unregister_callback_not_registered():
    jobs_process = JobsProcess()
    callback_called = False
    def callback(job_id, job_status):
        nonlocal callback_called
        callback_called = True
    with pytest.raises(Exception) as e:
        jobs_process.unregister_callback(callback)
    assert str(e.value) == f"Callback {callback} was not registered"

def test_on_thread_exception():
    jobs_process = JobsProcess()
    ex = Exception("Test exception")
    jobs_process.on_thread_exception(1, ex)
    assert jobs_process._exceptions[1] == ex

def test_on_thread_finish():
    jobs_process = JobsProcess()
    jobs_process.on_thread_finish(1)
    # no assert statement is needed as this method only logs a message

def test_process_pipeline_append_continue():
    jobs_process = JobsProcess()
    pipeline = (1, 2, [])
    jobs_process.process_pipeline(pipeline)
    assert 1 in jobs_process._jobs

def test_process_pipeline_clear_restart():
    jobs_process = JobsProcess()
    pipeline = (1, 2, [], "CLEAR_RESTART")
    jobs_process.process_pipeline(pipeline)
    assert 1 in jobs_process._jobs

def test_process_pipeline_bad_format():
    jobs_process = JobsProcess()
    pipeline = (1, 2)
    with pytest.raises(Exception) as e:
        jobs_process.process_pipeline(pipeline)
    assert str(e.value) == "pipeline_getter must return tuple of type (job_id, update_id, jobs) or (job_id, update_id, jobs, handling_type)"

def test_process_pipeline_unsupported_handling():
    jobs_process = JobsProcess()
    pipeline = (1, 2, [], "UNSUPPORTED_HANDLING")
    with pytest.raises(Exception) as e:
        jobs_process.process_pipeline(pipeline)
    assert str

def test_job_handling():
    assert libjob.JobHandling.CLEAR_RESTART == libjob.JobHandling.make("CLEAR_RESTART")
    assert libjob.JobHandling.APPEND_CONTINUE == libjob.JobHandling.make("APPEND_CONTINUE")
    assert libjob.JobHandling.CLEAR_RESTART == libjob.JobHandling.make(libjob.JobHandling.CLEAR_RESTART)
    assert libjob.JobHandling.APPEND_CONTINUE == libjob.JobHandling.make(libjob.JobHandling.APPEND_CONTINUE)
