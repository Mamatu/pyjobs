from pyjobs.private import libjob
from unittest.mock import MagicMock

def test_libjob():
    pipeline = [MagicMock(), MagicMock(), MagicMock()]
    jp = libjob.JobsProcess()
    job_id = 0
    update_id = 0
    jp.process_pipeline((job_id, update_id, pipeline))
    jp.wait_for_finish(job_id)
    print(pipeline)
    for i in pipeline: i.assert_called_once()
