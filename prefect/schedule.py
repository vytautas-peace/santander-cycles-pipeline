"""
Prefect deployment definition + weekly schedule.
Run: python schedule.py to register the deployment in Prefect Cloud.
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from pipeline import ingestion_flow

deployment = Deployment.build_from_flow(
    flow=ingestion_flow,
    name="weekly-ingestion",
    schedule=CronSchedule(cron="0 3 * * 1", timezone="Europe/London"),  # Mon 03:00
    parameters={"file_limit": None, "tmp_dir": "/tmp/tfl"},
    tags=["santander-cycles", "ingestion"],
    description="Weekly batch ingestion of TfL Santander Cycles CSV data",
)

if __name__ == "__main__":
    deployment.apply()
    print("Deployment registered: santander-cycles-ingestion/weekly-ingestion")
