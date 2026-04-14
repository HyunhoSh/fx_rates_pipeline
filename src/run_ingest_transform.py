from ingest import run_ingestion
from transform import run_transform


def run_pipeline() -> None:
    """Run the full pipeline: ingestion -> transformation"""
    print("Starting ingestion...")
    run_ingestion()

    print("Starting transformation...")
    run_transform()

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()