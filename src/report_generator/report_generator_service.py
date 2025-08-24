from sqlmodel.ext.asyncio.session import AsyncSession
from src.db.models import Stores,StoreUpdates,StoreBusinessHours
from sqlmodel import select 
from src.logging.logger import global_logger
from uuid import UUID 
from datetime import datetime,time,timedelta 
from zoneinfo import ZoneInfo
import pandas as pd 
import time as t
from pathlib import Path
from src.db.pqtimescale_client import get_session_context_manager
from src.db.redis_client import set_key


result_path=Path(__file__).parent.parent.parent/"result_data"

class ReportGeneratorService:
    
    async def get_store(self,store_id: UUID, session: AsyncSession):
        """
            It Fetches Individual Store from Stores Table
        Args:
            store_id (UUID): _description_
            session (AsyncSession): _description_

        Returns:
            _type_: Store
        """
        try:
            store_result = await session.exec(select(Stores).where(Stores.store_id == store_id))
            return store_result.first()
        except Exception as e:
            await global_logger.log_event(
                data={
                    "message": "error_fetching_store_from_db",
                    "error": str(e),
                    "store_id": str(store_id)
                },
                level="error"
            )
            raise 
        
    @staticmethod
    def get_defaults(store_id: UUID):
        """
            This is an Utility function to get default config for Store_id
            like timezone , business_hrs(24*7)
        Args:
            store_id (UUID): _description_

        Returns:
            _type_: Dict[Store],Dict[TimeZone],Dict[BusinessHrs]
        """
        store_dict = {
            "store_id": store_id
        }
        timezone_dict = {
            "timezone": "America/Chicago"
        }
        business_hrs_dict = {}
        for i in range(7):
            business_hrs_dict[i] = {
                "dayOfWeek": i,
                "start_time_local": time(0, 0),
                "end_time_local": time(23, 59)
            }
        return store_dict, timezone_dict, business_hrs_dict
    
    @staticmethod
    async def get_all_stores(session: AsyncSession):
        """
            Gets All Stores from Stores Table
        Args:
            session (AsyncSession): _description_

        Returns:
            _type_: List[Store]
        """
        try:
            stores_result = await session.exec(select(Stores))
            return stores_result.all()
        except Exception as e:
            await global_logger.log_event(
                data={
                    "message": "error_occurred_fetching_stores",
                    "error": str(e),
                },
                level="error"
            )
            raise
    
    @staticmethod
    async def get_store_business_hours(store_id: UUID, session: AsyncSession):
        """
        Takes in store_id and fetches Business_hrs from respective StoreBusinessHours
        table
        Args:
            store_id (UUID): _description_
            session (AsyncSession): _description_

        Returns:
            _type_: List[BusinessHours]
        """
        try:
            store_business_hrs = await session.exec(select(StoreBusinessHours).where(StoreBusinessHours.store_id == store_id))
            all_business_hours = store_business_hrs.all()
            return all_business_hours
        except Exception as e:
            await global_logger.log_event(
                data={
                    "message": "error_occurred_fetching_business_hrs",
                    "error": str(e),
                    "store_id": str(store_id)
                },
                level="error"
            )
            raise
    
    
    async def get_store_updates_within_range(self,start_time: datetime, end_time: datetime, store_id: UUID, session: AsyncSession):
        """
            Takes in time interval and get store status records from DB
        Args:
            start_time (datetime): _description_
            end_time (datetime): _description_
            store_id (UUID): _description_
            session (AsyncSession): _description_

        Returns:
            _type_: List[StoreUpdates]
        """
        try:
            updates_result = await session.exec(
                select(StoreUpdates)
                .where(StoreUpdates.store_id == store_id)
                .where(StoreUpdates.timestamp_utc >= start_time)
                .where(StoreUpdates.timestamp_utc <= end_time)
                .order_by(StoreUpdates.timestamp_utc)
            )

            # Fetch all the results
            store_updates = updates_result.all()
            return store_updates

        except Exception as e:
            await global_logger.log_event(
                data={
                    "message": "error_occurred_fetching_store_updates_within_range",
                    "error": str(e),
                    "store_id": str(store_id),
                    "start_time": str(start_time),
                    "end_time": str(end_time),
                },
                level="error"
            )
            raise    
    
    async def get_range_status(self,store_id:UUID,end_time:datetime,session:AsyncSession):
        """
            Takes in store_id,end_time 
            Get status records of store for last 1 weeks
        Args:
            store_id (UUID): _description_
            end_time (datetime): _description_
            session (AsyncSession): _description_

        Returns:
            _type_: DataFrame of Store Updates Records 
        """
        try:
            start_time=end_time-timedelta(weeks=1)
            results=await self.get_store_updates_within_range(start_time=start_time,end_time=end_time,store_id=store_id,session=session)
            if not results:
                return pd.DataFrame([]) 
            obs_data=[result.model_dump(exclude=["store_id","uid"]) for result in results]
            obs_df = pd.DataFrame(obs_data)
            obs_df["timestamp_utc"] = pd.to_datetime(obs_df["timestamp_utc"], utc=True)
            obs_df = obs_df.sort_values("timestamp_utc").reset_index(drop=True)
            return obs_df
        except Exception as e:
            print(e)
    
    async def get_store_details(self,store_id: UUID, session:AsyncSession):
        """
        Retrieves store details including timezone and business hours.

        Args:
            store_id (UUID): _description_
            session (AsyncSession): _description_

        Returns:
            A tuple containing:
                - The store object (Stores).
                - The timezone string (str), defaulting to "America/Chicago" if missing.
                - A list of business hours (list of StoreBusinessHours objects),
                filling in missing days with 00:00:00 to 23:59:59.
        """
        try:
            statement=select(Stores).where(Stores.store_id == store_id)
            store_result = await session.exec(statement)
            store = store_result.first()
            if not store:
                return None, None, None 
            
            timezone_str={"timezone":store.timezone.timezone} if store.timezone.timezone else {"timezone":"America/Chicago"}
            business_hours = store.business_hours or []
            bh_map = {bh.dayOfWeek for bh in business_hours}
            all_days = set(range(7))
            missing_days = list(all_days - bh_map)

            for day in missing_days:
                #setting day timeline to default (00:00 to 23:59)
                default_bh = StoreBusinessHours(
                    store_id=store_id,
                    dayOfWeek=day,
                    start_time_local=time(0, 0, 0),
                    end_time_local=time(23, 59, 59)
                )
                business_hours.append(default_bh)

            business_hours.sort(key=lambda bh: (bh.dayOfWeek, bh.start_time_local))

            business_hours_dict=[
                business_hr.model_dump(exclude="store_id") for business_hr in business_hours
            ]
            
            store_dict=store.model_dump()
            return store_dict,timezone_str, business_hours_dict

        except Exception as e:
            await global_logger.log_event(
                data={
                    "message": "error_fetching_store_details",
                    "error": str(e),
                    "store_id": str(store_id)
                },
                level="error"
            )
            raise
        
    @staticmethod    
    def build_status_intervals(observations: pd.DataFrame, now_utc: pd.Timestamp):
        """
            Convert point observations into step-function intervals:
            [t_i, t_{i+1}) with status_i, and last [t_last, now_utc].
        Args:
            observations (pd.DataFrame): _description_
            now_utc (pd.Timestamp): _description_

        Returns:
            Returns a list of dicts with 'start', 'end', 'status' (UTC tz-aware).
        """
        obs_df = observations.sort_values("timestamp_utc").reset_index(drop=True)
        if obs_df.empty:
            return []
        intervals = []
        for i in range(len(obs_df) - 1):
            start = obs_df.loc[i, "timestamp_utc"]
            end = obs_df.loc[i + 1, "timestamp_utc"]
            status = obs_df.loc[i, "status"]
            if end > start:
                intervals.append({"start": start, "end": end, "status": status})
        # Last open interval closes at now_utc
        last_start = obs_df.loc[len(obs_df) - 1, "timestamp_utc"]
        if now_utc > last_start:
            intervals.append({"start": last_start, "end": now_utc, "status": obs_df.loc[len(obs_df) - 1, "status"]})
        elif now_utc == last_start:
            pass
        return intervals
    
    @staticmethod
    def daterange(start_date, end_date):
        """Yield dates from start_date to end_date inclusive."""
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(days=n)

    @staticmethod
    def build_bh_intervals_utc(business_hrs_df: pd.DataFrame, tz: ZoneInfo, start_utc: pd.Timestamp, end_utc: pd.Timestamp):
        """
            Expand business hours to UTC intervals covering [start_utc, end_utc].
            Handles overnight (end <= start → rolls to next local day).
        Args:
            business_hrs_df (pd.DataFrame): _description_
            tz (ZoneInfo): _description_
            start_utc (pd.Timestamp): _description_
            end_utc (pd.Timestamp): _description_

        Returns:
            Returns list of (bh_start_utc, bh_end_utc).
        """
        bh_map = {}
        for _, row in business_hrs_df.iterrows():
            bh_map.setdefault(int(row["dayOfWeek"]), []).append((row["start_time_local"], row["end_time_local"]))

        start_local = start_utc.astimezone(tz)
        end_local = end_utc.astimezone(tz)
        local_start_date = start_local.date()
        local_end_date = end_local.date()
        intervals = []
        for d in ReportGeneratorService.daterange(local_start_date, local_end_date):
            dow = (d.weekday())
            if dow not in bh_map:
                continue
            for st_local_t, en_local_t in bh_map[dow]:
                st_local_dt = datetime.combine(d, st_local_t, tzinfo=tz)
                en_local_dt = datetime.combine(d, en_local_t, tzinfo=tz)
                if en_local_dt <= st_local_dt:
                    # Overnight case: end on next day
                    en_local_dt = en_local_dt + timedelta(days=1)
                st_utc = st_local_dt.astimezone(ZoneInfo("UTC"))
                en_utc = en_local_dt.astimezone(ZoneInfo("UTC"))
                # adjusting to requested window
                st = max(st_utc, start_utc)
                en = min(en_utc, end_utc)
                if en > st:
                    intervals.append((st, en))
        intervals.sort(key=lambda x: x[0])
        return intervals
    
    @staticmethod
    def intersect(a_start, a_end, b_start, b_end):
        """Return intersection [max(starts), min(ends)) or None if empty."""
        s = max(a_start, b_start)
        e = min(a_end, b_end)
        if e > s:
            return s, e
        return None

    @staticmethod
    def compute_uptime_downtime(observations: pd.DataFrame, bh_df: pd.DataFrame, tz: ZoneInfo,
                            now_utc: pd.Timestamp, window_td: timedelta):
        """
        Compute uptime/downtime seconds within business hours for a given rolling window ending at now_utc.
        Interpolation policy:
        - Step function carry-forward between observations
        - If no observation before window_start: back-fill from first observation at/after window_start
        - If no observations at all: return zeros
        
        Args:
            observations (pd.DataFrame): _description_
            bh_df (pd.DataFrame): _description_
            tz (ZoneInfo): _description_
            now_utc (pd.Timestamp): _description_
            window_td (timedelta): _description_

        Returns:
            (uptime_seconds, downtime_seconds)._
        """
        try:
            window_start = now_utc - window_td
            window_end = now_utc

            obs = observations.sort_values("timestamp_utc").reset_index(drop=True)
            if obs.empty:
                return 0.0, 0.0

            intervals = ReportGeneratorService.build_status_intervals(obs, now_utc)
            first_obs_time = obs.loc[0, "timestamp_utc"]
            if window_start < first_obs_time:
                # Back-fill status from the first as per our policy
                first_status = obs.loc[0, "status"]
                bf_end = min(first_obs_time, window_end)
                if bf_end > window_start:
                    intervals = [{"start": window_start, "end": bf_end, "status": first_status}] + intervals

            # Building BH intervals in UTC for this window(window_Start,window_End)
            bh_intervals = ReportGeneratorService.build_bh_intervals_utc(bh_df, tz, window_start, window_end)
            # print(pd.DataFrame(bh_intervals))
            # Intersect status intervals with BH intervals
            uptime = 0.0
            downtime = 0.0
            i = 0 
            for bh_start, bh_end in bh_intervals:
                while i < len(intervals) and intervals[i]["end"] <= bh_start:
                    i += 1
                j = i
                while j < len(intervals) and intervals[j]["start"] < bh_end:
                    seg = ReportGeneratorService.intersect(intervals[j]["start"], intervals[j]["end"], bh_start, bh_end)
                    if seg:
                        s, e = seg
                        dur = (e - s).total_seconds()
                        if intervals[j]["status"] == "active":
                            uptime += dur
                        else:
                            downtime += dur
                    j += 1
            return uptime, downtime
        except Exception as e:
            print(e)
            
    @staticmethod
    def summarize_results(uptime_sec, downtime_sec, window_td, units="minutes"):
        """
        Convert seconds to desired units and format.
        For last_hour: units="minutes" 
        For last_day/week: units="hours" 
        """
        total = uptime_sec + downtime_sec
        if total == 0:
            if units == "minutes":
                return 0, 0
            else:
                return 0.0, 0.0

        if units == "minutes":
            return int(round(uptime_sec / 60.0)), int(round(downtime_sec / 60.0))
        elif units == "hours":
            return round(uptime_sec / 3600.0, 2), round(downtime_sec / 3600.0, 2)
        else:
            raise ValueError("Unsupported units")

    async def build_report_per_store(self,store_id:UUID,session:AsyncSession,NOW_UTC:datetime):
        # -----------------------------
        # Run computations for the three windows
        # -----------------------------
        try:
            store,timezone_str,business_hrs=await self.get_store_details(
                                                                        store_id=store_id,
                                                                        session=session 
                                                                        )
            # print(store,timezone_str,business_hrs)
            if not store:
                store,timezone_str,business_hrs=ReportGeneratorService.get_defaults(
                    store_id=store_id  
                )
                
            bh_df = pd.DataFrame(business_hrs)
            last_hour_td = timedelta(hours=1)
            last_day_td = timedelta(days=1)
            last_week_td = timedelta(days=7)
            obs_df=await self.get_range_status(
                store_id=store_id,
                end_time=NOW_UTC,
                session=session
            )
            if obs_df.empty:
                return None 
            tz=ZoneInfo(timezone_str.get('timezone'))
            uh, dh = ReportGeneratorService.compute_uptime_downtime(obs_df, bh_df, tz, NOW_UTC, last_hour_td)
            ud, dd = ReportGeneratorService.compute_uptime_downtime(obs_df, bh_df, tz, NOW_UTC, last_day_td)
            uw, dw = ReportGeneratorService.compute_uptime_downtime(obs_df, bh_df, tz, NOW_UTC, last_week_td)
            uptime_last_hour_min, downtime_last_hour_min = ReportGeneratorService.summarize_results(uh, dh, last_hour_td, units="minutes")
            uptime_last_day_hr,  downtime_last_day_hr  = ReportGeneratorService.summarize_results(ud, dd, last_day_td,  units="hours")
            uptime_last_week_hr, downtime_last_week_hr = ReportGeneratorService.summarize_results(uw, dw, last_week_td, units="hours")

            results={
                "store_id": store_id,
                "uptime_last_hour_min": uptime_last_hour_min,
                "downtime_last_hour_min": downtime_last_hour_min,
                "uptime_last_day_hr": uptime_last_day_hr,
                "downtime_last_day_hr": downtime_last_day_hr,
                "uptime_last_week_hr": uptime_last_week_hr,
                "downtime_last_week_hr": downtime_last_week_hr,
                "now_utc": NOW_UTC.strftime("%Y-%m-%d %H:%M:%S %Z")
                }
            return results
        except Exception as e:
            await global_logger.log_event(
                data={
                    "message":"error_occurred_build_report",
                    "error":str(e),
                    "store_id":store_id,
                    "now_utc":NOW_UTC
                }
            )
            raise  

    async def final_report(self,NOW_UTC:datetime,report_id:str):
        """ 
            Takes in MAX_UTC_TIME and report_id
            Computes the restaurant active/inactive meterics 
            for all stores in db 
            Generate a csv file
        """
        try:
            start_time=t.time()
            async with get_session_context_manager() as session:
                all_stores=await self.get_all_stores(session=session)
                all_store_ids=[store.store_id for store in all_stores]
                # print(all_store_ids)
                print(all_store_ids[0])
                print(f"Total stores in {len(all_store_ids)}")
                store_results=[]
                for store_id in all_store_ids:
                    result=await self.build_report_per_store(store_id=store_id,
                                                        session=session,
                                                        NOW_UTC=NOW_UTC
                                                        )
                    if result:
                        store_results.append(result)
                
                df = pd.DataFrame(store_results)
                df.to_csv(f"{result_path}/{report_id}.csv", index=False)
                
                end_time=t.time()
                print(f"Total Time taken to generate a report is {end_time-start_time}")
                # once the task is done set key to completed
                await set_key({
                    str(report_id):"completed"
                })
                print(f"Report has been generated successfully")
        except Exception as e:
            await global_logger.log_event(
                data={
                    "message":"error_occurred_in_final_report_generation",
                    "error":str(e),
                    "now_utc":NOW_UTC
                }
            )
            raise 
            
                
        