import pandas as pd
import plotnine as pn

import timezonefinder as tzf

airport_coords = (
    pd.read_csv("../db/truth/T_MASTER_CORD.csv")
    .set_index("AIRPORT")
    .query("AIRPORT_IS_CLOSED < 1 and AIRPORT_IS_LATEST")[
        ["LATITUDE", "LONGITUDE", "DISPLAY_AIRPORT_NAME"]
    ]
)

def prep_bts(start_date, end_date, filename):
    finder = tzf.TimezoneFinder()
    flights = pd.read_csv(filename)
    flights = flights[flights['OP_UNIQUE_CARRIER'] == 'WN']

    flights["OriginTimezone"] = flights["ORIGIN"].map(
        lambda origin: finder.timezone_at(
            lng=airport_coords.LONGITUDE[origin], lat=airport_coords.LATITUDE[origin]
        )
    )
    flights["DestTimezone"] = flights["DEST"].map(
        lambda origin: finder.timezone_at(
            lng=airport_coords.LONGITUDE[origin], lat=airport_coords.LATITUDE[origin]
        )
    )
    flights["Div1Timezone"] = flights["DIV1_AIRPORT"].map(
        lambda div: None if pd.isna(div) else finder.timezone_at(
            lng=airport_coords.LONGITUDE[div], lat=airport_coords.LATITUDE[div]
        )
    )

    flights["FL_DATE"] = pd.to_datetime(flights["FL_DATE"])
    flights = flights[flights['FL_DATE'].between(start_date, end_date)]

    assert len(flights.index) > 0, 'empty flights dataframe! is the selected date range available in the selected dataset?'

    def dep_time_to_utc(row):
        hour = row["CRS_DEP_TIME"] // 100
        minute = row["CRS_DEP_TIME"] % 100
        date = row["FL_DATE"]
        local_ts = pd.Timestamp(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=hour,
            minute=minute,
            tz=row["OriginTimezone"],
        )
        return local_ts.tz_convert("UTC")

    def arr_time_to_utc(row):
        hour = row["CRS_ARR_TIME"] // 100
        minute = row["CRS_ARR_TIME"] % 100
        # account for overnight flights
        date = row["FL_DATE"] + (pd.Timedelta(seconds=0) if row["CRS_ARR_TIME"] > row["CRS_DEP_TIME"] - 400 else pd.Timedelta(days=1))
        local_ts = pd.Timestamp(
            year=date.year,
            month=date.month,
            day=date.day,
            hour=hour,
            minute=minute,
            tz=row["DestTimezone"],
        )
        return local_ts.tz_convert("UTC")

    flights["ScheduledDepTimeUTC"] = flights.apply(dep_time_to_utc, axis=1)
    flights["ScheduledDepDateUTC"] = flights["ScheduledDepTimeUTC"].dt.date
    flights["ScheduledDepHourUTC"] = flights["ScheduledDepTimeUTC"].dt.hour
    flights["ScheduledDepTimePacific"] = flights["ScheduledDepTimeUTC"].dt.tz_convert(
        "America/Los_Angeles"
    )
    flights["ScheduledDepDatePacific"] = flights["ScheduledDepTimePacific"].dt.date
    flights["ScheduledDepHourPacific"] = flights["ScheduledDepTimePacific"].dt.hour
    flights["ScheduledArrTimeUTC"] = flights.apply(arr_time_to_utc, axis=1)
    flights["ScheduledArrDateUTC"] = flights["ScheduledArrTimeUTC"].dt.date
    flights["ScheduledArrHourUTC"] = flights["ScheduledArrTimeUTC"].dt.hour
    flights["ScheduledArrTimePacific"] = flights["ScheduledArrTimeUTC"].dt.tz_convert(
        "America/Los_Angeles"
    )
    flights["ScheduledArrDatePacific"] = flights["ScheduledArrTimePacific"].dt.date
    flights["ScheduledArrHourPacific"] = flights["ScheduledArrTimePacific"].dt.hour

    def get_actual_dep_time(row):
        if pd.isna(row['DEP_DELAY']):
            return pd.NA
        return row['ScheduledDepTimeUTC'] + pd.Timedelta(minutes=row['DEP_DELAY'])
    
    def get_actual_arr_time(row):
        if pd.isna(row['ARR_DELAY']):
            return pd.NA
        return row['ScheduledArrTimeUTC'] + pd.Timedelta(minutes=row['ARR_DELAY'])

    def get_optional_hour(row):
        if pd.isna(row):
            return pd.NA
        return row.hour
    
    flights['ActualDepTimeUTC'] = flights.apply(get_actual_dep_time, axis=1)
    flights['ActualArrTimeUTC'] = flights.apply(get_actual_arr_time, axis=1)

    flights['ActualDepHourUTC'] = flights['ActualDepTimeUTC'].apply(get_optional_hour)
    flights['ActualArrHourUTC'] = flights['ActualArrTimeUTC'].apply(get_optional_hour)

    return flights

class OutcomeComparison:
    def __init__(self, actual: pd.DataFrame, simulated: pd.DataFrame) -> None:
        self.actual = actual
        self.simulated = simulated
        self.simulated['ARR_DELAY'] = (self.simulated['arr_time'] - self.simulated['sched_arr']).dt.total_seconds() // 60
        self.simulated['DEP_DELAY'] = (self.simulated['dep_time'] - self.simulated['sched_dep']).dt.total_seconds() // 60
        self.simulated.rename(columns={'cancelled': 'CANCELLED'}, inplace=True)
        self.merged = pd.merge(self.actual, self.simulated, on='id', suffixes=['_act', '_sim'])

    def compare_otp(self):
        return self.compare(lambda x: x['ARR_DELAY'].agg(lambda x: (x < 15).mean()))
    
    def compare_total_delay(self):
        return self.compare(lambda x: x['DEP_DELAY'].sum())
    
    def plot_actual_vs_sim_delay(self):
        # Requires each to have an `id` column.
        return pn.ggplot(self.merged) + pn.geom_point(pn.aes(x='ARR_DELAY_act', y='ARR_DELAY_sim')) + pn.theme_bw()

    def compare_num_cancellations(self):
        return self.compare(lambda x: x['CANCELLED'].sum())
    
    def num_different_aircraft(self):
        return (self.merged['tail'] != self.merged['TAIL_NUM']).sum()
    
    def very_different_depdelay(self):
        different = (self.merged['DEP_DELAY_act'] - self.merged['DEP_DELAY_sim']).abs() >= 30
        return self.merged[different]

    def compare(self, metric):
        actual = metric(self.actual)
        sim = metric(self.simulated)
        return dict(actual=actual, sim=sim)