import logging

def setup_logging(log_level=logging.INFO):
    """Configure logging for the application"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('tlc_pipeline')