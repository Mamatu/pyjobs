from unittest.mock import MagicMock

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
