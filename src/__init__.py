from fastapi import FastAPI,APIRouter,status 
from fastapi.responses import JSONResponse
from src.report_generator.routes import report_router
from src.utils.errors import register_error_handlers

version="v1"

app=FastAPI(
    title="LOOP.AI REPORT GENERATION SERVICE",
    description="A REST API Service to Generate RESTAURANTS USECASE REPORTS",
    version=version,
    docs_url=f"/api/{version}/docs",
    contact={
        "email":"vipulc2580@gmail.com"
    }    
)

main_router=APIRouter()

@main_router.get('/')
async def home():
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message":"Welcome to LOOP.AI REPORT GENERATION SERVICE"
        } 
    )
    
@main_router.get('/health')
async def health_check():
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message":"Health is OK"
        }
    )

register_error_handlers(app=app)

app.include_router(main_router,prefix="",tags=["home"])
app.include_router(report_router,prefix="/reports",tags=["reports"])
