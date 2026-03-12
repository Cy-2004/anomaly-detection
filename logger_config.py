import logging
import os

os.makedirs("logs", exist_ok=True)

LOG_FILE = "logs/app.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("anomaly_pipeline")