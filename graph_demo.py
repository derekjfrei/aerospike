"""
Top-level demo runner. Delegates to `graph.demo.run_demo()` which contains the
refactored graph demo logic.
"""
import logging
from graph import demo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        demo.run_demo()
    except Exception as e:
        logger.error(f"Error running graph demo: {e}")
        raise


if __name__ == "__main__":
    main()