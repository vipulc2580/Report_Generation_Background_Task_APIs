from typing import Any,Callable 
from fastapi import FastAPI,status 
from fastapi.requests import Request
from fastapi.responses import JSONResponse 
from datetime import datetime 
from src.logging.logger import global_logger


class CustomException(Exception):
    """
        This is Base Class for all CustomException/Error
    Args:
        Exception (_type_): _description_
    """
    pass 

class InternalServerError(CustomException):
    """ Internal Server Error Occurred"""
    pass 

class ReportAlreadyTrigger(CustomException):
    """ Report Already Triggered Exception"""
    pass 

def create_exception_handler(status_code:int,data:Any)->Callable:
    """this will return error handler function"""
    async def exception_handler(request:Request,exc:CustomException)->JSONResponse:
        await global_logger.log_event(
            data={
                "msg":f"Some_Error_occured",
                "error":str(exc),
                "timestamp":datetime.now() 
            },
            level="error"
        )
        return JSONResponse(status_code=status_code,content=data)
    
    return exception_handler

def register_error_handlers(app:FastAPI):
    app.add_exception_handler(
        ReportAlreadyTrigger,
        create_exception_handler(
            status_code=status.HTTP_400_BAD_REQUEST,
            data={
                "message":"Report is already triggered"
            }
        )
    )
    
    app.add_exception_handler(
        InternalServerError,
        create_exception_handler(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            data={
                "error":"Oops! Error occurred"
            }
        )
    )
    
    