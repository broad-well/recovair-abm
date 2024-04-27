from typing import Generator
import pandas as pd
import timezonefinder as tzf
import sqlite3
from queue import PriorityQueue
import math

airport_coords = (
    pd.read_csv("../truth/T_MASTER_CORD.csv")
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
    
    flights['ActualDepTimeUTC'] = flights.apply(get_actual_dep_time, axis=1)
    flights['ActualArrTimeUTC'] = flights.apply(get_actual_arr_time, axis=1)

    return flights

def get_airline_airport_capacities(df: pd.DataFrame):
    departures = df.groupby(['ORIGIN', 'FL_DATE', 'ScheduledDepHourUTC']).agg({'DEST': 'count'}).reset_index()\
        .groupby('ORIGIN').agg({'DEST': 'max'}).reset_index()\
        .rename(columns={'DEST': 'departures', 'ORIGIN': 'airport'})
    arrivals = df.groupby(['DEST', 'FL_DATE', 'ScheduledDepHourUTC']).agg({'ORIGIN': 'count'}).reset_index()\
        .groupby('DEST').agg({'ORIGIN': 'max'}).reset_index()\
        .rename(columns={'ORIGIN': 'arrivals', 'DEST': 'airport'})
    return pd.merge(departures, arrivals, on='airport')
    


def get_aircraft_types(path: str, aircraft_ref: str):
    df = pd.read_csv(path)
    acft = pd.read_csv(aircraft_ref)
    df = df[df['NAME'].str.strip().str.contains('SOUTHWEST AIRLINES')]
    df['N-NUMBER'] = df['N-NUMBER'].str.strip().map(lambda x: 'N' + x)
    # https://support.southwest.com/helpcenter/s/article/airplane-specifications
    df = df[['N-NUMBER', 'MFR MDL CODE']].rename(columns={'MFR MDL CODE': 'CODE'}).set_index('CODE')
    acft = acft[['CODE', 'MODEL']].set_index('CODE')
    acft['MODEL'] = acft['MODEL'].str.strip()
    type_table = df.join(acft)
    def model_to_type(model):
        if model.startswith('737-7'):
            return 'B737'
        elif model == '737-8':
            return 'B73M'
        elif model.startswith('737-8'):
            return 'B738'
    type_capacities = {
        'B737': 143,
        'B738': 175,
        'B73M': 175
    }
    type_table['TYPE'] = type_table['MODEL'].map(model_to_type)
    type_table['CAPACITY'] = type_table['TYPE'].map(type_capacities.get)
    return type_table

def add_initial_locations(acft, df):
    start_loc = df.sort_values('ScheduledDepTimeUTC').groupby('TAIL_NUM').agg({'ORIGIN': 'first'})
    merge = pd.merge(
        start_loc.reset_index().rename(columns={'TAIL_NUM': 'N-NUMBER', 'ORIGIN': 'LOCATION'}),
        acft.reset_index(),
        how='left',
        on='N-NUMBER')
    merge['TYPE'].fillna('B738', inplace=True)
    merge['CAPACITY'].fillna(175, inplace=True)
    return merge


class PassengerItinerarySynthesizer:
    def __init__(self, market, segment) -> None:
        self.market_df = market
        self.segment_df = segment
        self.distance_memo = {}
    
    # credit: pi.ai
    def haversine_distance(self, lat1, lon1, lat2, lon2, earth_radius=6371):
        # Conversion factors
        pi = math.pi
        dtor = pi / 180

        # Convert latitudes and longitudes into radians
        phi1 = lat1 * dtor
        phi2 = lat2 * dtor
        lam1 = lon1 * dtor
        lam2 = lon2 * dtor

        # Calculate the distance
        dlat = (phi2 - phi1) / 2
        dlon = (lam2 - lam1) / 2
        a = math.sin(dlat)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlon)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = earth_radius * c
        return distance

    def airport_distance(self, origin, dest):
        return self.distance_memo.setdefault((min(origin, dest), max(origin, dest)), self.haversine_distance(
            airport_coords.LATITUDE[origin],
            airport_coords.LONGITUDE[origin],
            airport_coords.LATITUDE[dest],
            airport_coords.LONGITUDE[dest]))

    def search_path(self, origin, dest):
        def backtrack(visited, start=dest):
            if start == origin: return [start]
            return backtrack(visited, start=visited[start][0]) + [start]

        frontier = PriorityQueue()
        frontier.put((0, origin, 0))
        visited = {}
        while not frontier.empty():
            cost, here, hops = frontier.get()
            if hops > 2:
                continue
            if here == dest:
                return backtrack(visited)
            for neighbor in self.segment_df[self.segment_df.ORIGIN == here].DEST:
                newcost = cost + self.airport_distance(here, neighbor)
                if neighbor not in visited: frontier.put((newcost, neighbor, hops + 1))
                if neighbor not in visited or visited[neighbor][1] > newcost:
                    visited[neighbor] = (here, newcost)

    def generate_itineraries(self):
        for _, row in self.market_df.iterrows():
            path = self.search_path(row.ORIGIN, row.DEST)
            if path:
                yield path, row.PASSENGERS


class DatabaseWriter:
    conn: sqlite3.Connection
    sid: str

    def __init__(self, _file, sid) -> None:
        self.conn = sqlite3.connect(_file)
        self.conn.execute('PRAGMA foreign_keys = ON;')
        self.sid = sid

    def write_scenario(self, name: str, start: str, end: str):
        self.conn.execute("DELETE FROM scenarios WHERE sid = ?", (self.sid,))
        self.conn.execute("INSERT INTO scenarios(sid, name, start_time, end_time) VALUES (?,?,?,?)",
                          (self.sid, name, start, end))

    def write_airports(self, arpt):
        args = ((row['airport'], row['departures'], row['arrivals'], airport_coords.LATITUDE[row['airport']], airport_coords.LONGITUDE[row['airport']], self.sid) for _, row in arpt.iterrows())
        self.conn.executemany("INSERT INTO airports(code, max_dep_per_hour, max_arr_per_hour, latitude, longitude, sid) "
                         "VALUES (?,?,?,?,?,?)", args)

    def write_aircraft(self, acft):
        args = ((row['N-NUMBER'], row['LOCATION'], row['TYPE'], row['CAPACITY'], self.sid) for _, row in acft.iterrows())
        self.conn.executemany("INSERT INTO aircraft(tail, location, typename, capacity, sid) VALUES(?,?,?,?,?)", args)

    def write_crew(self, crew):
        args = ((row['id'], row['location'], self.sid) for _, row in crew.iterrows())
        self.conn.executemany("INSERT INTO crew(id, location, sid) VALUES(?,?,?)", args)

    def write_flights(self, df):
# INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid)
# VALUES (4, 'WN881', 'N941WN', 'DAL', 'MDW', NULL, "2024-03-01 16:45:00", "2024-03-01 19:25:00", 'few');
        args = (
            (i, row['OP_CARRIER_FL_NUM'], row['TAIL_NUM'],
             row['ORIGIN'], row['DEST'], None,
             row['ScheduledDepTimeUTC'].strftime('%Y-%m-%d %H:%M:%S'),
             row['ScheduledArrTimeUTC'].strftime('%Y-%m-%d %H:%M:%S'),
             self.sid) for i, row in df.iterrows())
        self.conn.executemany("INSERT INTO flights(id, flight_number, aircraft, origin, dest, pilot, sched_depart, sched_arrive, sid) VALUES(?,?,?,?,?,?,?,?,?)",
                              args)
        
    def write_synthesized_itineraries(self, market_file, segment_file, carrier, days):
        import tqdm
        market_df = pd.read_csv(market_file)
        segment_df = pd.read_csv(segment_file)
        synth = PassengerItinerarySynthesizer(
            market_df[market_df.CARRIER == carrier],
            segment_df[segment_df.CARRIER == carrier])

        self.conn.executemany("INSERT INTO demand(path, amount, sid) VALUES(?,?,?)",
                              tqdm.tqdm(('-'.join(path), int(amount / 30 * days), self.sid) for path, amount in synth.generate_itineraries()))


def synthesize_crew(df, mult=1.4):
    assigned = df[~df['TAIL_NUM'].isna()]
    def airport_initial_crew_count(assigned, airport: str) -> int:
        """Generate a graph of crew-team counts at an airport."""
        day_flights = assigned[(assigned.ORIGIN == airport) | (assigned.DEST == airport)]\
            .sort_values('ScheduledDepTimeUTC')
        events = sorted(list(day_flights.apply(lambda x: row_to_event(x, airport), axis=1)))
        counts = [0]
        for _time, change, _flight_number in events:
            counts.append(counts[-1] + change)
        # plt.show()
        return -min(counts)

    def row_to_event(row: pd.Series, airport: str):
        """Convert one BTS dataset row to an event in the form (time, crew change [+1/-1])"""
        arrival = row['DEST'] == airport
        if arrival:
            return (row['ScheduledArrTimeUTC'], 1, (row['OP_CARRIER_FL_NUM'],))
        else:
            return (row['ScheduledDepTimeUTC'], -1, (row['OP_CARRIER_FL_NUM'],))
    
    airports = assigned.ORIGIN.unique()
    _id = 0
    crew = []
    for airport in airports:
        for i in range(int(mult * airport_initial_crew_count(assigned, airport))):
            crew.append(({'id': _id, 'location': airport}))
            _id += 1
    return pd.DataFrame(crew)

if __name__ == '__main__':
    # chosen for having zero cancellations that day (congrats southwest)
    df = prep_bts('2024-01-28', '2024-01-28', '../truth/T_ONTIME_REPORTING.csv')
    # for i in df['ORIGIN'].unique():
    #     print(i)
    acft = get_aircraft_types('../truth/MASTER.txt', '../truth/ACFTREF.txt')
    writer = DatabaseWriter("test.db", "jan28-bts-import")
    writer.write_scenario("January 28 BTS", "2024-01-28 00:00:00", "2024-01-29 20:00:00")
    arpts = get_airline_airport_capacities(df)
    writer.write_airports(arpts)
    acft = add_initial_locations(acft, df)
    writer.write_aircraft(acft)
    writer.write_crew(synthesize_crew(df, mult=2))
    writer.write_flights(df)
    writer.write_synthesized_itineraries(
        '../truth/T_T100D_MARKET_US_CARRIER_ONLY.csv',
        '../truth/T_T100D_SEGMENT_US_CARRIER_ONLY.csv',
        'WN', days=1.5)
    writer.conn.commit()
    writer.conn.close()