from src import app 
from src.db.schemas import StoreBusinessHoursCreate,StoreTimeZoneCreate,StoreUpdatesCreate
from src.db.pqtimescale_client import get_session_context_manager
from src.db.models import StoreBusinessHours,StoreTimeZones,Stores,StoreUpdates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
import pandas as pd 
import uuid 
from datetime import time 
from sqlmodel import select 
from src.logging.logger import global_logger 
from pathlib import Path 
import os 
import asyncio

MAX_PARAMS = 32767

async def ingest_stores_from_csv(file_path: str, session: AsyncSession):
    try:
        invalid_count = 0
        created_count = 0
        
        df = pd.read_csv(file_path)
        
        unique_ids = df["store_id"].unique()

        for sid in unique_ids:
            try:
                sid_uuid = uuid.UUID(sid)
                new_store = Stores(
                    store_id=sid_uuid,
                    store_name=f"Store-{sid[:6]}"
                )
                session.add(new_store)
                created_count += 1
            except Exception:
                invalid_count += 1
                
        await session.commit()
        status={
                "message": "stores_ingested",
                "created_count": created_count,
                "invalid_count": invalid_count,
                "total_seen": len(unique_ids)
            }
        await global_logger.log_event(
            data=status,
            level="info"
        )
        print(f"Status of Store Creation Script :{status}")
    except Exception as e:
        await session.rollback()
        await global_logger.log_event(
            data={
                "error":str(e),
                "message":"error_creating_stores"
            },
            level="error"
        )
            
    
async def ingest_business_hours_from_csv(file_path: str, session: AsyncSession):
    
    try:
        df = pd.read_csv(file_path)
        df.columns = ["store_id", "dayOfWeek", "start_time_local", "end_time_local"]

        # Convert start/end times to datetime.time
        df["start_time_local"] = pd.to_datetime(df["start_time_local"], format="%H:%M:%S", errors="coerce").dt.time

        df["end_time_local"] = pd.to_datetime(df["end_time_local"], format="%H:%M:%S", errors="coerce").dt.time

        # Counters
        total_rows = len(df)
        invalid_rows = 0
        duplicate_rows = 0
        incomplete_hours = 0

        # Step 3: Fetch all stores from DB
        stores_result = await session.exec(select(Stores))
        all_stores = stores_result.all()

        # Step 4: Build lookup from CSV → dict[store_id][dayOfWeek] = (start, end)
        bh_map = {}
        for _, row in df.iterrows():
            sid = str(row["store_id"])
            day = row["dayOfWeek"]

            # Basic validation
            if pd.isna(sid):
                invalid_rows += 1
                continue

            if sid not in bh_map:
                bh_map[sid] = {}
            if day in bh_map[sid]:
                duplicate_rows += 1
                continue  
            bh_map[sid][day] = (row["start_time_local"], row["end_time_local"])

        # Step 5: Ingest for each store
        for store in all_stores:
            sid = str(store.store_id)
            existing_days = bh_map.get(sid, {})

            for day in existing_days.keys():
                start, end = existing_days[day]
                session.add(StoreBusinessHours(
                    store_id=store.store_id,
                    dayOfWeek=day,
                    start_time_local=start,
                    end_time_local=end
                ))

        await session.commit()
        print("✅ Business hours ingested successfully")

        # Log summary
        await global_logger.log_event(
                data={
                    "message": "business_hours_ingestion_summary",
                    "total_rows": total_rows,
                    "invalid_rows": invalid_rows,
                    "duplicate_rows": duplicate_rows,
                    "stores_with_incomplete_hours": incomplete_hours
                },
                level="info"
            )
    except Exception as e:
        await session.rollback()
        await global_logger.log_event(
            data={
                "error": str(e),
                "message": "error_occurred_in_business_hours_ingestion"
            },
            level="error"
        )
    
async def ingest_store_timezones(file_path:str,session:AsyncSession):
    try:
        # Step 1: Read CSV
        df = pd.read_csv(file_path, header=None)
        df.columns = ["store_id", "timezone_str"]

        total_rows = len(df)
        duplicate_rows = 0
        invalid_rows = 0
        missing_timezones = 0

        # Drop duplicates
        before = len(df)
        df = df.drop_duplicates(subset=["store_id"], keep="first")
        duplicate_rows = before - len(df)

        # Validation: filter out missing store_id or timezone
        df = df.dropna(subset=["store_id", "timezone_str"])
        after_validation = len(df)
        invalid_rows = before - after_validation - duplicate_rows

        # Step 2: Build lookup map
        tz_map = {str(row["store_id"]): row["timezone_str"] for _, row in df.iterrows()}

        # Step 3: Fetch all stores
        stores_result = await session.exec(select(Stores))
        all_stores = stores_result.all()

        for store in all_stores:
            sid = str(store.store_id)
            tz = tz_map.get(sid)
            if not tz:
                missing_timezones += 1
                
            session.add(StoreTimeZones(
                store_id=store.store_id,
                timezone=tz
            ))

        await session.commit()
        print("Timezones ingested successfully")

        # Log summary
        await global_logger.log_event(
            data={
                "message": "timezone_ingestion_summary",
                "total_rows": total_rows,
                "duplicate_rows": duplicate_rows,
                "invalid_rows": invalid_rows,
                "stores_with_missing_timezones": missing_timezones
            },
            level="info"
        )

    except Exception as e:
        await session.rollback()
        await global_logger.log_event(
            data={
                "error": str(e),
                "message": "error_occurred_in_store_timezone_ingestion"
            },
            level="error"
        )
        
async def ingest_store_status_records(file_path: str, session: AsyncSession, batch_size: int = 7000):
    total_rows = 0
    duplicate_rows = 0
    invalid_rows = 0
    inserted_rows = 0

    try:
        # --- preload valid store_ids from stores table ---
        result = await session.exec(select(Stores.store_id))
        valid_store_ids = set(result.all())   # .all() gives list of store_ids
        valid_store_ids=[str(valid_store_id) for valid_store_id in valid_store_ids]
        for chunk in pd.read_csv(file_path, chunksize=batch_size):
            chunk.columns = ["store_id", "status", "timestamp_utc"]
            total_rows += len(chunk)

            before = len(chunk)
            chunk = chunk.drop_duplicates(subset=["store_id", "timestamp_utc"], keep="first")
            duplicate_rows += before - len(chunk)

            chunk = chunk.dropna(subset=["store_id", "status", "timestamp_utc"])
            after_validation = len(chunk)
            invalid_rows += before - after_validation

            chunk["timestamp_utc"] = pd.to_datetime(
                chunk["timestamp_utc"], utc=True, errors="coerce"
            )
            chunk = chunk.dropna(subset=["timestamp_utc"])
            chunk["store_id"] = chunk["store_id"].astype(str)
            # --- Filter invalid store_ids ---
            before_filter = len(chunk)
            chunk = chunk[chunk["store_id"].isin(valid_store_ids)]
            invalid_rows += before_filter - len(chunk)

            if chunk.empty:
                continue

            rows = chunk.to_dict(orient="records")

            # --- enforce safe sub-batching ---
            num_cols = len(rows[0]) if rows else 1
            max_rows_per_insert = MAX_PARAMS // num_cols

            for i in range(0, len(rows), max_rows_per_insert):
                batch = rows[i : i + max_rows_per_insert]

                stmt = insert(StoreUpdates).values(batch)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["store_id", "timestamp_utc"]
                )

                await session.exec(stmt)
                await session.commit()

                inserted_rows += len(batch)
                # Checking inserting of records
                print(f"Inserted rows so far: {inserted_rows}")

        print("Store status records ingestion completed")

        await global_logger.log_event(
            data={
                "message": "store_status_records_ingestion_summary",
                "total_rows": total_rows,
                "duplicate_rows": duplicate_rows,
                "invalid_rows": invalid_rows,
                "inserted_rows": inserted_rows,
            },
            level="info"
        )

    except Exception as e:
        await session.rollback()
        await global_logger.log_event(
            data={
                "error": str(e),
                "message": "error_occurred_in_store_status_records_ingestion"
            },
            level="error"
        )
        
async def main():
    print("Script started")
    data_path = Path(__file__).parent.parent / "store_data"

    async with get_session_context_manager() as session:
        await ingest_stores_from_csv(os.path.join(data_path, "store_status.csv"), session=session)
        # await ingest_business_hours_from_csv(os.path.join(data_path, "menu_hours.csv"), session=session)
        # await ingest_store_timezones(os.path.join(data_path, "timezones.csv"), session=session)
        # await ingest_store_status_records(os.path.join(data_path, "store_status.csv"), session=session)
        
if __name__=="__main__":
    asyncio.run(main())
        
