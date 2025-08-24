from pydantic_settings import BaseSettings,SettingsConfigDict
from pathlib import Path 

class Configuration(BaseSettings):
    DATABASE_URL:str 
    REDIS_URL:str 
    LOGGER_SERVICE:str 
    
    model_config=SettingsConfigDict(
        env_file=Path(__file__).parent.parent/".env"
    )
    
Config=Configuration()

broker_url=Config.REDIS_URL
result_backend=Config.REDIS_URL
broker_connection_retry_on_startup=True 
