# We ignore everything in this file due to issues with setters being a different type than their property. Guido ranked
# this as a top concern so might get fixed eventually. The issue for reference:
# https://github.com/python/mypy/issues/3004.
# type: ignore
from __future__ import annotations

import base64
import json
import os

import boto3
from cryptography.hazmat.primitives import serialization

from ocpipes import logger

secretsmanager = boto3.client("secretsmanager", region_name="us-east-1")


def get_secret(secret_id: str) -> dict[str, str]:
    """Decrypts and retrieves the specified secret from AWS Secrets Manager.

    Args:
        secret_id (str): The name of the secret.

    Returns:
        dict[str, str]: The secret key/value mappings.
    """
    response = secretsmanager.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


class ConnectionSecrets:
    """Base class for service connection information.

    Credentials can be optionally overwritten with precedence:
        1) Uses any value that's manually specified.
        2) Checks environment variables and loads if possible.
        3) Checks AWS Secrets Manager and loads if possible.
        4) Uses default value.
    """

    def __init__(self, secret_id: str) -> None:
        self._secrets = get_secret(secret_id=secret_id)

    def _get_value_env_secret_or_default(
        self, value: str | int | None, env_key: str | None, secret_key: str | None, default_val: str | int | None
    ) -> str | int | None:
        if value:
            return value
        elif env_key is not None and env_key in os.environ:
            return os.environ.get(env_key)
        elif secret_key is not None and secret_key in self._secrets:
            return self._secrets.get(secret_key)

        return default_val


class SnowflakeConnectionSecrets(ConnectionSecrets):
    """Holds Snowflake credentials and connection information.

    We use Key Pair Authentication for both user-specific and system (production) connections. An RSA public key is
    shared across all DS-provisioned users within Snowflake, which are federated via Okta. The same public key and its
    corresponding private key, plus password to decrypt, are stored in AWS Secrets Manager. This allows for both
    IAM-based access control within the DS AWS account and those compute resources, plus the ability to have
    user-specific connections for query introspection and cost attribution.
    See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html#key-pair-authentication-key-pair-rotation
    """

    _env_user = "SNOWFLAKE_USER"
    _env_role = "SNOWFLAKE_ROLE"
    _env_account = "SNOWFLAKE_ACCOUNT"
    _env_warehouse = "SNOWFLAKE_WAREHOUSE"
    _env_database = "SNOWFLAKE_DATABASE"
    _env_schema = "SNOWFLAKE_SCHEMA"

    _secret_id = "arn:aws:secretsmanager:us-east-1:786346568665:secret:DS_Snowflake-KFiHoP"
    _secret_public_key = "RSA_2048_Public_Key_PEM_Base64"
    _secret_private_key = "RSA_2048_Private_Key_PEM_Base64"
    _secret_encryption_password = "ENCRYPTION_PASSWORD"

    _default_user_dev = None
    _default_user_prod = "SYSTEM_DATA_SCIENCE_USER"
    _default_role = "PRODUCER_DATASCIENCE_ROLE"
    _default_account = "oxa55065"
    _default_warehouse_dev = "DATASCIENCE_WH"
    _default_warehouse_prod = "SYSTEM_DATASCIENCE_WH"
    _default_database_dev = "_DEV_TEAM_DATASCIENCE"
    _default_database_prod = "TEAM_DATASCIENCE"
    _default_schema = "PUBLIC"

    def __init__(
        self,
        user: str | None = None,
        role: str | None = None,
        account: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        is_production: bool = False,
    ) -> None:
        super().__init__(self._secret_id)
        self.is_production = is_production
        self.user = user
        self.role = role
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\n"
            f"    user={self.user}\n"
            f"    role={self.role}\n"
            f"    account={self.account}\n"
            f"    warehouse={self.warehouse}\n"
            f"    database={self.database}\n"
            f"    schema={self.schema}\n"
            f")"
        )

    def get_private_key(self) -> bytes:
        private_key_base64_bytes = self._secrets.get(self._secret_private_key).encode()

        rsa_private_key = serialization.load_pem_private_key(
            base64.b64decode(private_key_base64_bytes),
            password=self._secrets.get(self._secret_encryption_password).encode(),
        )

        return rsa_private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @property
    def user(self) -> str:
        return self._user

    @user.setter
    def user(self, value: str | None):
        user = self._get_value_env_secret_or_default(value, self._env_user, None, self._default_user_dev)

        if not self.is_production and not user:
            raise ValueError(
                f"Missing {self._env_user} environment variable in a non-production runtime. Check your local `.env` "
                "file and ensure it is available in the devcontainer by running `env | grep USER` in your VSCode "
                "integrated terminal. If running on Batch, ensure the environment variable is passed using "
                f'`@environment(vars={{"SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER")}})`'
            )

        # Default to system user in production.
        if self.is_production:
            # Only warn when the user value is explicitly passed, ignore when inferred from env.
            if user == value:
                logger.warning(
                    f"Snowflake user {user} was specified in production. Ignoring and using {self._default_user_prod}"
                )
            user = self._default_user_prod

        self._user = user

    @property
    def role(self) -> str:
        return self._role

    @role.setter
    def role(self, value: str | None):
        self._role = self._get_value_env_secret_or_default(value, self._env_role, None, self._default_role)

    @property
    def account(self) -> str:
        return self._account

    @account.setter
    def account(self, value: str | None):
        self._account = self._get_value_env_secret_or_default(value, self._env_account, None, self._default_account)

    @property
    def warehouse(self) -> str:
        return self._warehouse

    @warehouse.setter
    def warehouse(self, value: str | None):
        # Default to prod warehouse when inferred via system user and is production.
        default_warehouse = self._default_warehouse_dev
        if self.is_production and self.user == self._default_user_prod:
            default_warehouse = self._default_warehouse_prod

        self._warehouse = self._get_value_env_secret_or_default(value, self._env_warehouse, None, default_warehouse)

    @property
    def database(self) -> str:
        return self._database

    @database.setter
    def database(self, value: str | None):
        # Default to prod database when inferred via system user and is production.
        default_database = self._default_database_dev
        if self.is_production and self.user == self._default_user_prod:
            default_database = self._default_database_prod

        self._database = self._get_value_env_secret_or_default(value, self._env_database, None, default_database)

    @property
    def schema(self) -> str:
        return self._schema

    @schema.setter
    def schema(self, value: str | None):
        self._schema = self._get_value_env_secret_or_default(value, self._env_schema, None, self._default_schema)


class SlackConnectionSecrets(ConnectionSecrets):
    """Holds the Slack OAuth token for the Metaflow bot user."""

    _secret_id = "arn:aws:secretsmanager:us-east-1:786346568665:secret:DS_Slack-zs4MzR"
    _secret_token = "token"

    def __init__(self) -> None:
        super().__init__(self._secret_id)
        self._token = self._secrets.get(self._secret_token)

    @property
    def token(self) -> str:
        return self._token


class PineconeConnectionSecrets(ConnectionSecrets):
    """Holds the Pinecone API token."""

    _secret_id = "arn:aws:secretsmanager:us-east-1:786346568665:secret:DS_Pinecone-3XLguG"
    _secret_api_key_default = "default"

    def __init__(self) -> None:
        super().__init__(self._secret_id)
        self._api_key_default = self._secrets.get(self._secret_api_key_default)

    @property
    def api_key_default(self) -> str:
        return self._api_key_default


class DBConnectionSecrets(ConnectionSecrets):
    """Holds database credentials and connection information."""

    _env_ds_db_username = "OCDBUNAME"
    _env_ds_db_password = "OCDBPWD"
    _env_ds_db_engine = "OCDBENGINE"
    _env_ds_db_host = "OCDBHOST"
    _env_ds_db_port = "OCDBPORT"
    _env_ds_db_dbname = "OCDBNAME"

    _secret_id = "arn:aws:secretsmanager:us-east-1:786346568665:secret:DS-DB-Root-User-NRvuGd"
    _secret_ds_db_username = "username"
    _secret_ds_db_password = "password"
    _secret_ds_db_engine = "engine"
    _secret_ds_db_host = "host"
    _secret_ds_db_port = "port"
    _secret_ds_db_dbname = "dbname"

    _default_ds_db_username = "opcity"
    _default_ds_db_password = None
    _default_ds_db_engine = "postgresql+psycopg2"
    _default_ds_db_host = "ds-db-cluster.cluster-cbnawlmjvuti.us-east-1.rds.amazonaws.com"
    _default_ds_db_port = 5432
    _default_ds_db_dbname = "opcity"

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        engine: str | None = None,
        host: str | None = None,
        port: str | int | None = None,
        dbname: str | None = None,
    ) -> None:
        super().__init__(self._secret_id)
        self.username = username
        self.password = password
        self.engine = engine
        self.host = host
        self.port = port
        self.dbname = dbname
        logger.warning(
            "Deprecated - the nightly restored Referral Aurora Postgres DB will be removed soon. Please refactor any "
            "queries to use Snowflake!"
        )

    @property
    def username(self) -> str:
        return self._username

    @username.setter
    def username(self, value: str | None):
        self._username = self._get_value_env_secret_or_default(
            value, self._env_ds_db_username, self._secret_ds_db_username, self._default_ds_db_username
        )

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, value: str | None):
        self._password = self._get_value_env_secret_or_default(
            value, self._env_ds_db_password, self._secret_ds_db_password, self._default_ds_db_password
        )

    @property
    def engine(self) -> str:
        return self._engine

    @engine.setter
    def engine(self, value: str | None):
        self._engine = self._get_value_env_secret_or_default(
            value, self._env_ds_db_engine, self._secret_ds_db_engine, self._default_ds_db_engine
        )

    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, value: str | None):
        self._host = self._get_value_env_secret_or_default(
            value, self._env_ds_db_host, self._secret_ds_db_host, self._default_ds_db_host
        )

    @property
    def port(self) -> str:
        return self._port

    @port.setter
    def port(self, value: str | None):
        self._port = self._get_value_env_secret_or_default(
            value, self._env_ds_db_port, self._secret_ds_db_port, self._default_ds_db_port
        )

    @property
    def dbname(self) -> str:
        return self._dbname

    @dbname.setter
    def dbname(self, value: str | None):
        self._dbname = self._get_value_env_secret_or_default(
            value, self._env_ds_db_dbname, self._secret_ds_db_dbname, self._default_ds_db_dbname
        )
