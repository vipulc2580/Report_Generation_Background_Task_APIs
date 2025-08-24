from pydantic import BaseModel,Field,field_validator,model_validator
import uuid 
from datetime import time,datetime,timezone,timedelta   

class StoreBusinessHoursCreate(BaseModel):
    store_id:uuid.UUID 
    dayOfWeek:int 
    start_time_local:time 
    end_time_local:time 
    
    # here i want validation on day week(0,6)
    # and time being between 00:00:00 and 23:59:59
    @field_validator("dayOfWeek")
    def validate_day_of_week(cls,v):
        if v<0 or v>6:
            raise ValueError("dayOfWeek must be between 0 (Monday) and 6(Sunday)")
        return v 
    
    @field_validator("start_time_local","end_time_local")
    def validate_time_range(cls,v:time):
        if v<time(0,0,0) or v>time(23,59,59):
            raise ValueError('Time must be between 00:00:00 and 23:59:59')
        return v 
    
    @model_validator(mode="after")
    def validate_time_order(cls, values):
        start = values.start_time_local
        end = values.end_time_local

        if start == end:
            raise ValueError("start_time_local and end_time_local cannot be equal")
        
        return values
    
class StoreTimeZoneCreate(BaseModel):
    store_id:uuid.UUID 
    timezone:str 
    
    
class StoreUpdatesCreate(BaseModel):
    store_id:uuid.UUID 
    timestamp_utc:datetime # ( we need check that it needs to be in utc_timezone)
    status:str # it can only have two values active/inactive
    
    @field_validator("timestamp_utc")
    def validate_utc(cls,v:datetime):
        if v.tzinfo is None or v.utcoffset() != timedelta(0):
            raise ValueError("timestamp_utc must be timezone-aware and in utc")
        return v 
    
    @field_validator('status')
    def validate_status(cls,v:str):
        allowed={"active","inactive"}
        if v.lower() not in allowed:
            raise ValueError(f"Status must be one of {allowed}")
        return v.lower()
    
        