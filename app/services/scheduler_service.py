from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.ingestion_service import IngestionService


scheduler = BackgroundScheduler()


def run_scheduled_ingestion():
    db: Session = SessionLocal()
    try:
        print("Running scheduled ingestion...")
        result = IngestionService.process_files(db)
        print("Scheduled ingestion completed:", result)
    except Exception as e:
        print("Scheduled ingestion failed:", str(e))
    finally:
        db.close()


def start_scheduler():
    scheduler.add_job(
        run_scheduled_ingestion,
        trigger=CronTrigger(hour=6, minute=0),  # Schedule for 6:00 AM daily
        id="daily_ingestion_job",
        replace_existing=True,
    )
    scheduler.start()
    print("Scheduler started. Daily ingestion set for 6:00 AM.")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        print("Scheduler stopped.")
