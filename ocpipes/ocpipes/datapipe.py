from datetime import datetime, timedelta
from time import sleep
from typing import Optional

import boto3
import pytz
from tenacity import retry
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

from ocpipes import logger
from ocpipes.secrets import DBConnectionSecrets

DS_DB_ROOT_USER_SECRET = "DS-DB-Root-User"
DS_DB_CLUSTER_IDENTIFIER = "ds-db-cluster"
DS_DB_INSTANCE_IDENTIFIER = "ds-db"
# The parameter groups have tweaks specific to the DS account, such as significantly increased query workspace memory,
# temp buffers, and temp file limits.
DS_DB_CLUSTER_PARAMETER_GROUP_NAME = "ds-cluster-aurora-postgresql12"
DS_DB_INSTANCE_PARAMETER_GROUP_NAME = "ds-aurora-postgresql12"
DS_DB_INSTANCE_CLASS = "db.r6g.4xlarge"

ENG_SHARED_DB_CLUSTER_IDENTIFIER = "prod-db-cluster"
ENG_SHARED_DB_CLUSTER_SNAPSHOT_IDENTIFIER = "prod-db-cluster-ds"
NUM_OLD_SNAPSHOTS_TO_RETAIN = 3

rds = boto3.client("rds", region_name="us-east-1")


def db_exists(cluster_identifier: str = DS_DB_CLUSTER_IDENTIFIER) -> bool:
    """Check if there is a DB cluster with the specified identifier.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier. Defaults to DS_DB_CLUSTER_IDENTIFIER.

    Returns:
        bool: If the Aurora DB cluster exists.
    """
    try:
        rds.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        cluster_exists = True
    except rds.exceptions.DBClusterNotFoundFault:
        cluster_exists = False
    return cluster_exists


def get_db_status(cluster_identifier: str = DS_DB_CLUSTER_IDENTIFIER) -> tuple[str, Optional[str], Optional[str]]:
    """List of status: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Status.html.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier. Defaults to DS_DB_CLUSTER_IDENTIFIER.

    Returns:
        Tuple[str, Optional[str], Optional[str]]: The status, endpoint and host of the Aurora DB cluster,
            e.g. ('available', 'ds-db-cluster.cluster-cbnawlmjvuti.us-east-1.rds.amazonaws.com', '5432').
    """
    try:
        cluster_description = rds.describe_db_clusters(DBClusterIdentifier=cluster_identifier)["DBClusters"][0]
        status = cluster_description.get("Status")
        endpoint = cluster_description.get("Endpoint")
        port = cluster_description.get("Port")
    except rds.exceptions.DBClusterNotFoundFault:
        status = "deleted"
        endpoint, port = None, None
    return status, endpoint, port


def get_db_snapshots_by_recency(
    cluster_identifier: str = ENG_SHARED_DB_CLUSTER_IDENTIFIER,
    snapshot_identifier_prefix: str = ENG_SHARED_DB_CLUSTER_SNAPSHOT_IDENTIFIER,
) -> list[dict]:
    """Return all snapshots for the RDS DB cluster with the given prefix.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier. Defaults to ENG_SHARED_DB_CLUSTER_IDENTIFIER.
        snapshot_identifier_prefix (str, optional): Defaults to ENG_SHARED_DB_CLUSTER_SNAPSHOT_IDENTIFIER.

    Returns:
        List[Dict]: All snapshots for the RDS DB cluster, ordered most recent first.
    """
    return sorted(
        [
            s
            for s in rds.describe_db_cluster_snapshots(DBClusterIdentifier=cluster_identifier)["DBClusterSnapshots"]
            if s["DBClusterSnapshotIdentifier"].startswith(snapshot_identifier_prefix)
        ],
        key=lambda s: s.get("SnapshotCreateTime", datetime.utcnow().replace(tzinfo=pytz.utc)),
        reverse=True,
    )


def delete_old_snapshots(
    cluster_identifier: str = ENG_SHARED_DB_CLUSTER_IDENTIFIER,
    snapshot_identifier_prefix: str = ENG_SHARED_DB_CLUSTER_SNAPSHOT_IDENTIFIER,
):
    """Used to clean up old snapshots that are shared with the DS account.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier for the snapshots. Defaults to
        ENG_SHARED_DB_CLUSTER_IDENTIFIER.
        snapshot_name_prefix (str, optional): Additional check before deleting. Defaults to
        ENG_SHARED_DB_CLUSTER_SNAPSHOT_IDENTIFIER.
    """
    snapshots = get_db_snapshots_by_recency(cluster_identifier, snapshot_identifier_prefix)

    old_snapshots = snapshots[NUM_OLD_SNAPSHOTS_TO_RETAIN:]
    for old_snapshot in old_snapshots:
        old_snapshot_identifier = old_snapshot["DBClusterSnapshotIdentifier"]
        logger.info(f"Deleting snapshot {old_snapshot_identifier}")
        rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=old_snapshot_identifier)


@retry(stop=stop_after_attempt(3), retry=retry_if_exception_type(AssertionError))
def apply_db_modifications(cluster_identifier: str = DS_DB_CLUSTER_IDENTIFIER):
    """Resets the root DB password, updates the parameter group and reduces backup retention period to minimum.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier. Defaults to DS_DB_CLUSTER_IDENTIFIER.
    """
    try:
        # Reset the master password so we can use the opcity user.
        logger.info("Fetching DB root credentials from AWS Secrets Manager")
        connection_info = DBConnectionSecrets()

        logger.info("Resetting DB root password")
        rds.modify_db_cluster(
            DBClusterIdentifier=cluster_identifier,
            ApplyImmediately=True,
            BackupRetentionPeriod=1,
            MasterUserPassword=connection_info.password,
        )

        # The password update only affects cluster-level status and not DB instance availability.
        # There isn't an RDS waiter for `db_cluster_available` so we'll wait and check the event logs.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#waiters
        sleep(120)

    except rds.exceptions.InvalidDBClusterStateFault as e:
        if "resetting-master-credentials" in e.response["Error"]["Message"]:
            logger.info("Already resetting the DB root password.")
            sleep(120)
        elif "backing-up" in e.response["Error"]["Message"]:
            status, endpoint, port = get_db_status(cluster_identifier)
            logger.info(f"DB cluster is not available for modification with status {status} at: {endpoint}:{port}")
            sleep(300)
        else:
            raise

    response = rds.describe_events(
        SourceIdentifier=cluster_identifier,
        SourceType="db-cluster",
        StartTime=datetime.now() - timedelta(hours=1),
        EndTime=datetime.now(),
    )

    validate_messages = {
        "Reset master credentials": False,
    }

    for rds_event in response["Events"]:
        if rds_event["Message"] in validate_messages:
            validate_messages[rds_event["Message"]] = True

    assert all(validate_messages.values()), "Missing RDS modifications"


def do_db_restore(
    cluster_identifier: str = DS_DB_CLUSTER_IDENTIFIER,
    instance_identifier: str = DS_DB_INSTANCE_IDENTIFIER,
    instance_class: str = DS_DB_INSTANCE_CLASS,
    cluster_parameter_group: str = DS_DB_CLUSTER_PARAMETER_GROUP_NAME,
    instance_parameter_group: str = DS_DB_INSTANCE_PARAMETER_GROUP_NAME,
):
    """Restores the DB cluster using the most recent available snapshot.

    Additionally applies the standard updates as described in `apply_db_modifications`

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier for the snapshots. Defaults to
        ENG_SHARED_DB_CLUSTER_IDENTIFIER.
        instance_identifier (str, optional): The RDS DB instance identifier. Defaults to DS_DB_INSTANCE_IDENTIFIER.
        instance_class (str, optional): _description_. Defaults to DS_DB_INSTANCE_CLASS.
        cluster_parameter_group (str, optional): The name of the DB cluster parameter group to associate with this
        DB cluster. Defaults to DS_DB_CLUSTER_PARAMETER_GROUP_NAME.
        instance_parameter_group (str, optional): The name of the DB parameter group to associate with this DB instance.
          Defaults to DS_DB_INSTANCE_PARAMETER_GROUP_NAME.
    """
    logger.info(f"Checking for existing cluster {cluster_identifier}")
    if db_exists(cluster_identifier):
        logger.info("Cluster already exists")
    else:
        logger.info("No existing cluster found, creating")
        snapshots = get_db_snapshots_by_recency()
        assert len(snapshots) > 0, "No snapshots to restore from!"

        snapshot_id = snapshots[0]["DBClusterSnapshotIdentifier"]
        create_cluster_with_instance(
            snapshot_identifier=snapshot_id,
            cluster_identifier=cluster_identifier,
            instance_identifier=instance_identifier,
            instance_class=instance_class,
            cluster_parameter_group=cluster_parameter_group,
            instance_parameter_group=instance_parameter_group,
        )

    apply_db_modifications(cluster_identifier=cluster_identifier)
    logger.info("DB modifications complete")


def do_db_delete(
    cluster_identifier: str = DS_DB_CLUSTER_IDENTIFIER, instance_identifier: str = DS_DB_INSTANCE_IDENTIFIER
):
    """Deletes the specified cluster and instance, if the cluster exists.

    Args:
        cluster_identifier (str, optional): The RDS DB cluster identifier for the snapshots. Defaults to
        ENG_SHARED_DB_CLUSTER_IDENTIFIER.
        instance_identifier (str, optional): The RDS DB instance identifier. Defaults to DS_DB_INSTANCE_IDENTIFIER.
    """
    logger.info(f"Checking for existing cluster {cluster_identifier}")
    if db_exists(cluster_identifier):
        delete_cluster_with_instance(cluster_identifier, instance_identifier)
    else:
        logger.info("No existing cluster found")


def create_cluster_with_instance(
    snapshot_identifier: str,
    cluster_identifier: str,
    instance_identifier: str,
    instance_class: str,
    cluster_parameter_group: str,
    instance_parameter_group: str,
):
    """Creates a new DB cluster from a DB snapshot or DB cluster snapshot and creates a new DB instance in that cluster.

    Args:
        snapshot_identifier (str): The identifier for the snapshot to restore the DB from.
        cluster_identifier (str): The RDS DB cluster identifier for the snapshots.
        instance_identifier (str):  The identifier for the DB instance. This parameter is stored as a lowercase string.
        instance_class (str): The compute and memory capacity of the DB instance, for example db.m5.large.
        cluster_parameter_group (str): The name of the DB cluster parameter group to associate with this DB cluster.
        instance_parameter_group (str): The name of the DB parameter group to associate with this DB instance.
    """
    logger.info(f"Restoring snapshot {snapshot_identifier} to cluster {cluster_identifier}")
    rds.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=cluster_identifier,
        SnapshotIdentifier=snapshot_identifier,
        Engine="aurora-postgresql",
        EngineVersion="12.15",
        Port=5432,
        DBClusterParameterGroupName=cluster_parameter_group,
    )
    logger.info(f"Creating DB instance {instance_identifier}")
    rds.create_db_instance(
        DBInstanceIdentifier=instance_identifier,
        DBClusterIdentifier=cluster_identifier,
        DBInstanceClass=instance_class,
        Engine="aurora-postgresql",
        DBParameterGroupName=instance_parameter_group,
        PubliclyAccessible=False,
        EnablePerformanceInsights=True,
    )

    logger.info(f"Waiting on DB instance {instance_identifier} to become available")
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(DBInstanceIdentifier=instance_identifier, WaiterConfig={"Delay": 300, "MaxAttempts": 48})
    status, endpoint, port = get_db_status(cluster_identifier)
    logger.info(f"DB instance {instance_identifier} is {status} at: {endpoint}:{port}")


def delete_cluster_with_instance(cluster_identifier: str, instance_identifier: str):
    """Deletes the specified cluster and instance.

    Args:
        cluster_identifier (str): The RDS DB cluster identifier to be deleted.
        instance_identifier (str): The RDS DB instance identifier to be deleted.
    """
    logger.info(f"Deleting database instance {instance_identifier} in cluster {cluster_identifier}")
    rds.delete_db_instance(DBInstanceIdentifier=instance_identifier, SkipFinalSnapshot=True)
    logger.info(f"Deleting database cluster {cluster_identifier}")
    rds.delete_db_cluster(DBClusterIdentifier=cluster_identifier, SkipFinalSnapshot=True)

    logger.info(f"Waiting on DB instance {instance_identifier} to be deleted")
    waiter = rds.get_waiter("db_instance_deleted")
    waiter.wait(DBInstanceIdentifier=instance_identifier, WaiterConfig={"Delay": 30, "MaxAttempts": 60})
    logger.info(f"DB instance {instance_identifier} deleted")

    # The cluster itself takes a bit more time to be deleted, also need to check logs since there isn't a waiter for it.
    sleep(120)

    response = rds.describe_events(
        SourceIdentifier=cluster_identifier,
        SourceType="db-cluster",
        StartTime=datetime.now() - timedelta(hours=1),
        EndTime=datetime.now(),
    )

    validate_messages = {
        "DB cluster deleted": False,
    }

    for rds_event in response["Events"]:
        if rds_event["Message"] in validate_messages:
            validate_messages[rds_event["Message"]] = True

    assert all(validate_messages.values()), "Failed to validate DB cluster deleted"
    logger.info(f"DB cluster {cluster_identifier} deleted")
