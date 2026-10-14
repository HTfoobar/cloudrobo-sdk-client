import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from cloudrobo_core.sdk.obs_client import OBSClient


def test_upload_file_preserves_default_call_and_forwards_progress(tmp_path):
    path = tmp_path / "data"
    path.write_bytes(b"abc")
    obs = OBSClient.__new__(OBSClient)
    obs.bucket_name = "bucket"
    obs.client = MagicMock()
    obs.client.uploadFile.return_value = SimpleNamespace(status=200)
    assert obs.upload_file(str(path), "key")
    assert "progressCallback" not in obs.client.uploadFile.call_args.kwargs
    callback = MagicMock()
    assert obs.upload_file(
        str(path), "key", progress_callback=callback, checkpoint_file="checkpoint"
    )
    assert obs.client.uploadFile.call_args.kwargs["progressCallback"] is callback
    assert obs.client.uploadFile.call_args.kwargs["checkpointFile"] == "checkpoint"


def test_failed_multipart_abort_keeps_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text(json.dumps(dict(bucketName="bucket", objectKey="key", uploadId="upload")))
    obs = OBSClient.__new__(OBSClient)
    obs.client = MagicMock()
    obs.client.abortMultipartUpload.return_value = SimpleNamespace(status=500)
    assert not obs.abort_checkpoint_upload(str(path))
    assert path.exists()
    obs.client.abortMultipartUpload.return_value = SimpleNamespace(status=404)
    assert obs.abort_checkpoint_upload(str(path))
    assert not path.exists()
