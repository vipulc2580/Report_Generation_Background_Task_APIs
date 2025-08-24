from fastapi import Request,status,APIRouter,Depends
from fastapi.responses import JSONResponse
from datetime import datetime 
import uuid
from src.db.redis_client import get_value,set_key
from src.db.pqtimescale_client import get_session
from src.utils.errors import CustomException,ReportAlreadyTrigger,InternalServerError
from .report_generator_service import ReportGeneratorService 
from sqlmodel.ext.asyncio.session import AsyncSession
from uuid import UUID 
from datetime import datetime
from zoneinfo import ZoneInfo
from src.utils.celery_tasks import trigger_report_task
report_router=APIRouter()
report_generator_src=ReportGeneratorService()


@report_router.get('/trigger-report')
async def trigger_report(session:AsyncSession=Depends(get_session)):
    try:
        end_time=datetime(2024, 10, 15, 5, 26, 0, tzinfo=ZoneInfo("UTC"))        
        current_datetime=datetime.now().strftime("%d-%m-%Y %H:%M")
        # return already triggered
        value=await get_value(current_datetime)
        if value:
            raise ReportAlreadyTrigger()
        report_id= uuid.uuid4()
        # setting time : report_id
        await set_key({
            current_datetime:str(report_id)
        })
        # setting report_id : its status
        await set_key(
            {
                str(report_id):"running"
            }
        )
        # trigger report generation background task 
        trigger_report_task.delay(
            report_id=str(report_id),
            NOW_UTC=end_time.isoformat()  # ISO format is best for datetime
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message":"Report Generation has been started",
                "report_id":str(report_id)
            }
        )
    except CustomException as ce:
        raise 
    except Exception as e:
        print(e)
        raise  InternalServerError()
    
    
@report_router.post('/get-report')
async def get_report(report_id:uuid.UUID):
    try:
        # check if report is in running state 
        value=await get_value(str(report_id))
        if value==b"running":
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "status":"Running"
                }
            )
        else:
            # report is generated find s3 link or path and return so user can download it 
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "status":"Completed",
                    "report_link":rf"G:\Python\LOOP_AI_Task\result_data\{report_id}.csv"
                }
            )
    except Exception as e:
        raise InternalServerError()

    