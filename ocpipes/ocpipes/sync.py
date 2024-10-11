import subprocess
from threading import Event, Thread

from ocpipes import logger


def s3_sync(source: str, destination: str, timeout: int = None):
    """
    Syncs directories and S3 prefixes including deleting files. Recursively copies new and updated files from the source
    directory to the destination. Only creates folders in the destination if they contain one or more files. Both the
    source and destination may be an S3Uri.

    See: https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/sync.html

    Args:
        source (str): Local path or S3Uri.
        destination (str): Local path or S3Uri.
        timeout (int, optional): Max time in seconds. Defaults to None.
    """
    try:
        command = ["aws", "s3", "sync", source, destination]
        logger.debug(f"Syncing from {source} to {destination}.")
        response = subprocess.run(
            args=command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            timeout=timeout,
            check=False,
            text=True,
        )
        if response.stdout:
            logger.debug(response.stdout)
    except subprocess.TimeoutExpired as timeout_error:
        logger.error(f"Sync timeout after {timeout_error.timeout} seconds.")
        raise


class DaemonS3Sync(Thread):
    """
    Periodically sync local directory with S3 on a background thread.

    Examples:
        >>> syncer = DaemonS3Sync(local_path=".", s3_uri="s3://opcity-ds-scratch/.../dir")
        >>> syncer.start()
        >>> ... other code ...
        >>> syncer.stop()  # to ensure final sync, otherwise python kills the daemon thread at exit.

        >>> with DaemonS3Sync(local_path="catboost_info", s3_uri=f"s3://.../{metaflow_run_id}"):
        >>>     ... train code ...
    """

    def __init__(self, local_path: str, s3_uri: str, interval: int = 300, timeout: int = None):
        """
        Args:
            local_path (str): Local directory and its contents to sync.
            s3_uri (str): Destination in s3.
            interval (int, optional): Frequency to schedule sync in seconds. Defaults to 300.
            timeout (int, optional): Max time in seconds to attempt sync. Defaults to None.
        """
        super().__init__(daemon=True)
        self.local_path = local_path
        self.s3_uri = s3_uri
        self.interval = interval
        self.timeout = timeout
        self._stopped = Event()

    def sync_to_s3(self):
        s3_sync(source=self.local_path, destination=self.s3_uri, timeout=self.timeout)

    def sync_from_s3(self):
        s3_sync(source=self.s3_uri, destination=self.local_path, timeout=self.timeout)

    def run(self):
        """
        Start the periodic file sync. Immediately attempt to pull directory from s3, thereafter sync from local to s3.
        """
        self.sync_from_s3()
        while not self.is_stopped():
            self._stopped.wait(self.interval)
            self.sync_to_s3()

    def stop(self):
        self._stopped = self._stopped.set()

    def is_stopped(self):
        # Event becomes None after being stopped.
        if self._stopped is not None:
            return self._stopped.is_set()
        else:
            return True

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()

