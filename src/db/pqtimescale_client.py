from sqlmodel import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import sessionmaker 
from src.constants.config import Config
from contextlib import asynccontextmanager

engine=AsyncEngine(
    create_engine(
        url=Config.DATABASE_URL,
        echo=False 
    )
)


@asynccontextmanager
async def get_session_context_manager() -> AsyncSession:
    Session = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    async with Session() as session:
        try:
            yield session
        finally:
            await session.close()
            
            
async def get_session()->AsyncSession:
    Session=sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    async with Session() as session:
        yield session 