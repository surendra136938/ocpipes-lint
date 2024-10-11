import os
import yaml
import logging
import logging.config

LOG_CFG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "logging.yaml"))
LOG_CFG_OS_ENV_KEY = "LOG_CFG"


def setup_logging(default_path=LOG_CFG_PATH, default_level=logging.INFO, env_key=LOG_CFG_OS_ENV_KEY):
    """
    Setup logging configuration from yaml. Optional path override from environment variable.
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print("Error in Logging Configuration. Using default configs")
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print("Using default logging configs, no overrides found.")


setup_logging()
logger = logging.getLogger("ocpipes")
debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
critical = logger.critical
exception = logger.exception
