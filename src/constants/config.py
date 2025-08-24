from pydantic_settings import BaseSettings,SettingsConfigDict
from pathlib import Path 

class Configuration(BaseSettings):
    DATABASE_URL:str 
    REDIS_URL:str 
    LOGGER_SERVICE:str 
    MAIL_SERVER:str
    MAIL_PORT:int=587
    MAIL_USERNAME:str 
    MAIL_PASSWORD:str 
    MAIL_FROM:str 
    MAIL_FROM_NAME:str
    MAIL_STARTTLS:bool=True                        
    MAIL_SSL_TLS:bool=False                       
    USE_CREDENTIALS:bool=True                   
    VALIDATE_CERTS:bool=True
    DOMAIN:str
    
    model_config=SettingsConfigDict(
        env_file=Path(__file__).parent.parent/".env"
    )
    
Config=Configuration()

broker_url=Config.REDIS_URL
result_backend=Config.REDIS_URL
broker_connection_retry_on_startup=True 
