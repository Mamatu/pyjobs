from unittest.mock import MagicMock
import time

def test_libjob_one_job():
    from pyjobs.private import libjob
    pipeline = [MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_libjob_3_jobs():
    from pyjobs.private import libjob
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_libjob_1():
    from pyjobs.private import libjob
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    pipeline1 = [MagicMock(), MagicMock(), MagicMock()]
    jp = libjob.JobsProcess()
    update_id = 0
    jp.process_pipeline((0, update_id, pipeline))
    jp.process_pipeline((1, update_id, pipeline1))
    jp.wait_for_finish(0)
    for i in pipeline: i.assert_called_once()
    jp.wait_for_finish(1)
    for i in pipeline1: i.assert_called_once()

def test_lib_one_job():
    from pyjobs import lib
    pipeline = [MagicMock()]
    jp = lib.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_lib_3_jobs():
    from pyjobs import lib
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    jp = lib.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    for i in pipeline: i.assert_called_once()

def test_lib_1():
    from pyjobs import lib
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    pipeline1 = [MagicMock(), MagicMock(), MagicMock()]
    jp = lib.JobsProcess()
    update_id = 0
    jp.process_pipeline((0, update_id, pipeline))
    jp.process_pipeline((1, update_id, pipeline1))
    jp.wait_for_finish(0)
    for i in pipeline: i.assert_called_once()
    jp.wait_for_finish(1)
    for i in pipeline1: i.assert_called_once()

def test_lib_pipelines_one_job():
    from pyjobs import lib
    job_id = 0
    update_id = 0
    pipelines = {(job_id, update_id) : [MagicMock()]}
    jp = lib.JobsProcess()
    jp.process_pipelines(pipelines)
    jp.wait_for_finish(job_id)
    for i in pipelines[(0, 0)]: i.assert_called_once()

def test_lib_pipelines_3_jobs():
    from pyjobs import lib
    job_id = 0
    update_id = 0
    pipelines = {(job_id, update_id) : [MagicMock(), MagicMock(), MagicMock()]}
    jp = lib.JobsProcess()
    jp.process_pipelines(pipelines)
    jp.wait_for_finish(job_id)
    for i in pipelines[(0, 0)]: i.assert_called_once()

def test_lib_pipelines_1():
    from pyjobs import lib
    update_id = 0
    pipelines = {(0, update_id) : [MagicMock(), MagicMock(), MagicMock()], (1, update_id) : [MagicMock(), MagicMock(), MagicMock()]}
    jp = lib.JobsProcess()
    jp.process_pipelines(pipelines)
    jp.wait_for_finish(0)
    for i in pipelines[(0, 0)]: i.assert_called_once()
    jp.wait_for_finish(1)
    for i in pipelines[(1, 0)]: i.assert_called_once()

def test_lib_pipelines_stop_handler():
    from pyjobs import lib
    update_id = 0
    mock_stop_handler = MagicMock()
    mock_process = MagicMock()
    stopped = False
    jp = lib.JobsProcess()
    def mock_process_side_effect():
        while not stopped:
            time.sleep(0.01)
            jp.stop()
    def stop_handler_side_effect(stop = True):
        global stopped
        print("stop_handler_side_effect")
        stopped = True
    mock_process.side_effect = mock_process_side_effect
    mock_stop_handler.side_effect = stop_handler_side_effect
    pipelines = {(0, update_id) : [(mock_process, mock_stop_handler)], (1, update_id) : [MagicMock(), MagicMock(), MagicMock()]}
    jp.process_pipelines(pipelines)
    jp.wait_for_finish(0)
    jp.wait_for_finish(1)
    mock_process.assert_called_once()
    mock_stop_handler.assert_called_once()
    for i in pipelines[(1, 0)]: i.assert_called_once()
