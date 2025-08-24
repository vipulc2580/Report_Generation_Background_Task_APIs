import redis.asyncio as redis 
from src.constants.config import Config

from typing import Optional


EXP_TIME=3600

class RedisClient:
    """ SingleTon Class to get Redis Client"""
    _instance=None 
    
    @classmethod
    def get_instance(cls)->redis.Redis:
        if cls._instance is None:
            try:
                cls._instance=redis.from_url(
                    Config.REDIS_URL
                )
            except Exception as e:
                print(e)
        return cls._instance

async def get_value(key:str):
    """Gets the value from redis based on Key"""
    value=None
    try:
        if not key:
            return value 
        client=RedisClient.get_instance()
        value=await client.get(key)
        return value 
    except Exception as e:
        return value 
    
async def set_key(data)->None:
    """Add data to redis """
    if not data:
        return 
    try:
        client=RedisClient.get_instance()
        for key,value in data.items():
            await client.set(name=key, value=value, ex=EXP_TIME)
    except Exception as e:
        print(e)