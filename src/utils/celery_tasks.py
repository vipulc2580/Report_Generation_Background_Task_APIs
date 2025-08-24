from celery import Celery
import uuid 
from datetime import datetime
from asgiref.sync import async_to_sync
from src.db.pqtimescale_client import get_session
from sqlmodel.ext.asyncio.session import AsyncSession
import asyncio
from uuid import UUID 

celery_app=Celery()

celery_app.config_from_object('src.constants.config')

@celery_app.task()
def trigger_report_task(report_id: str, NOW_UTC: str) -> None:
    print("Background Task has started")

    # Convert back to original types
    report_id = uuid.UUID(report_id)
    # we utc back
    NOW_UTC = datetime.fromisoformat(NOW_UTC)

    from src.report_generator.report_generator_service import ReportGeneratorService
    report_generation_service = ReportGeneratorService()
    asyncio.run(report_generation_service.final_report(NOW_UTC=NOW_UTC, report_id=report_id))
    

