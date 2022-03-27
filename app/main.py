"""
GCN Test Task
"""

from app.pipelines import downloader_pipeline
from app.pipelines import reader_pipeline
from app.pipelines import database_pipeline


def main() -> None:
    """
    Main Generalized Function
    :return:
    """
    downloader_pipeline()
    records = reader_pipeline()
    database_pipeline(records)


if __name__ == "__main__":
    print("Starting application")
    print(__doc__)
    main()
    print("Application finished successfully")
