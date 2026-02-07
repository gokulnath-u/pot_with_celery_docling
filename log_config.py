import logging
import sys

def setup_logging():
    if logging.getLogger().hasHandlers():
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(processName)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
