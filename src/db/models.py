from sqlmodel import SQLModel,Field,Relationship
import sqlalchemy.dialects.postgresql as pg 
from datetime import datetime,date ,time 
from uuid import UUID,uuid4
from sqlalchemy import Column, String, Integer, Time, ForeignKey,UniqueConstraint,DateTime

""" 
Stores Table
    store_id: unique id (UUID)
"""
class Stores(SQLModel, table=True):
    __tablename__ = "stores"

    store_id: UUID = Field(
        sa_column=Column(pg.UUID(as_uuid=True), primary_key=True, default=uuid4)
    )
    
    # relationships
    timezone: "StoreTimeZones" = Relationship(back_populates="store",
                                            sa_relationship_kwargs={
                                                "lazy":"selectin"
                                            })
    business_hours: list["StoreBusinessHours"] = Relationship(back_populates="store",
                                                            sa_relationship_kwargs={
                                                           "lazy":"selectin"
                                                            })


""" 
Class StoreTimeZones:
    uid:unique id
    store_id:uuid(foreign key to StoreBuinessHours),
    timezone:str(default="America/Chicago" if not provided)
"""

class StoreTimeZones(SQLModel, table=True):
    __tablename__ = "store_time_zones"

    # store_id is both PRIMARY KEY and FOREIGN KEY
    store_id: UUID = Field(
        sa_column=Column(
            pg.UUID(as_uuid=True),
            ForeignKey("stores.store_id", ondelete="CASCADE"),
            primary_key=True,
        )
    )

    timezone: str = Field(
        sa_column=Column(String, nullable=True)
    )
    
    store: Stores = Relationship(back_populates="timezone")
    
""" 
Class StoreBusinessHours
    store_id (uuid4)
    dayOfWeek(0=Monday,6=Sunday)
    start_time_local, datetime(but timezone can vary assume it to be datetime)
    end_time_local  datetime(but timezone can vary assume it to be datetime)
"""


class StoreBusinessHours(SQLModel, table=True):
    __tablename__ = "store_business_hours"

    store_id: UUID = Field(
        sa_column=Column(pg.UUID(as_uuid=True),
                        ForeignKey("stores.store_id", ondelete="CASCADE"),
                        primary_key=True)
    )
    dayOfWeek: int = Field(
        sa_column=Column(Integer, primary_key=True, nullable=False)
    )
    start_time_local: time = Field(sa_column=Column(Time, nullable=True))
    end_time_local: time = Field(sa_column=Column(Time, nullable=True))

    store: Stores = Relationship(back_populates="business_hours")
    
    
""" 
Class StoreUpdates:
    uid:unique id 
    store_id:foregin_key
    timestamp_utc:datetime
    status:str
"""
class StoreUpdates(SQLModel,table=True):
    __tablename__ = "store_updates"
    __table_args__ = (
        UniqueConstraint("store_id", "timestamp_utc", name="uq_store_timestamp"),
    )

    uid: UUID = Field(
        sa_column=Column(pg.UUID(as_uuid=True), primary_key=True, default=uuid4)
    )

    store_id: UUID = Field(
        sa_column=Column(pg.UUID(as_uuid=True),
                        ForeignKey("stores.store_id", ondelete="CASCADE"),
                        nullable=False,
                        index=True)
    )

    timestamp_utc: datetime = Field(
        sa_column=Column(DateTime(timezone=True),
                        nullable=False,
                        default=datetime.utcnow,
                        index=True)
    )

    status: str = Field(sa_column=Column(String, nullable=False))

    