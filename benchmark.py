import pandas as pd
import time
import json
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from pymongo import MongoClient

# read nyc taxi data
print("Reading NYC taxi data...")
files = ['taxi_jan.parquet', 'taxi_feb.parquet', 'taxi_mar.parquet', 'taxi_apr.parquet']
df = pd.concat([pd.read_parquet(f) for f in files])
print(f"Total dataset: {len(df):,} rows")
print()

# connect to cassandra
cluster = Cluster(['127.0.0.1'])
cass = cluster.connect()
cass.execute("""
    CREATE KEYSPACE IF NOT EXISTS taxi
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
cass.set_keyspace('taxi')
cass.execute("DROP TABLE IF EXISTS trips")
cass.execute("""
    CREATE TABLE trips (
        sensor_id TEXT,
        timestamp TIMESTAMP,
        vendor_id INT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        pickup_location_id INT,
        dropoff_location_id INT,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        payment_type INT,
        PRIMARY KEY ((sensor_id), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
""")
print("Cassandra: ready")

# connect to mongodb
mongo = MongoClient('mongodb://127.0.0.1:27017/')
mdb = mongo['taxi']
mdb.trips.drop()
print("MongoDB: ready")
print()

# load 1 million rows into both databases as base data
print("Loading 1,000,000 rows into both databases as base data...")
print("This will take a few minutes...")
print()

df_base = df.head(1_000_000)

# mongodb load
records = df_base.to_dict('records')
for r in records:
    for k in list(r.keys()):
        if pd.isna(r[k]):
            r[k] = None
        elif hasattr(r[k], 'to_pydatetime'):
            r[k] = r[k].to_pydatetime()

batch_size = 5000
for i in range(0, len(records), batch_size):
    mdb.trips.insert_many(records[i:i+batch_size])
    done = i + batch_size
    if done % 200000 == 0:
        print(f"  MongoDB base load: {done:,} rows done")

mdb.trips.create_index('PULocationID')
mdb.trips.create_index('payment_type')
mdb.trips.create_index('tpep_pickup_datetime')
mdb.trips.create_index('fare_amount')
print(f"  MongoDB: {mdb.trips.count_documents({}):,} rows loaded, indexes created")
print()

# cassandra load
insert_cql = cass.prepare("""
    INSERT INTO trips (sensor_id, timestamp, vendor_id,
        passenger_count, trip_distance, pickup_location_id, dropoff_location_id,
        fare_amount, tip_amount, total_amount, payment_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

futures = []
cass_loaded = 0
for _, row in df_base.iterrows():
    try:
        future = cass.execute_async(insert_cql, (
            f"taxi_{int(row.get('PULocationID', 0) or 0)}",
            row.get('tpep_pickup_datetime'),
            int(row.get('VendorID', 0) or 0),
            float(row.get('passenger_count', 0) or 0),
            float(row.get('trip_distance', 0) or 0),
            int(row.get('PULocationID', 0) or 0),
            int(row.get('DOLocationID', 0) or 0),
            float(row.get('fare_amount', 0) or 0),
            float(row.get('tip_amount', 0) or 0),
            float(row.get('total_amount', 0) or 0),
            int(row.get('payment_type', 0) or 0)
        ))
        futures.append(future)
        cass_loaded += 1
    except:
        pass
    if len(futures) >= 5000:
        for f in futures:
            try: f.result()
            except: pass
        futures = []
        if cass_loaded % 200000 == 0:
            print(f"  Cassandra base load: {cass_loaded:,} rows done")

for f in futures:
    try: f.result()
    except: pass

print(f"  Cassandra: {cass_loaded:,} rows loaded")
print()
print("Base data loaded into both databases.")
print()


# -- DEMO 1: sequential time-series writes --
# simulating sensors sending data one record at a time
# 500,000 writes each

num_writes = 500_000
base_time = datetime(2025, 6, 1)

print("DEMO 1: Sequential Time-Series Writes")
print(f"Simulating sensors sending {num_writes:,} data points one at a time")
print("Cassandra uses async writes, MongoDB uses insert_one")
print()

# cassandra sequential async writes
print("  Running Cassandra writes...")
start = time.time()
futures = []

for i in range(num_writes):
    ts = base_time + timedelta(seconds=i)
    future = cass.execute_async(insert_cql, (
        f"sensor_{i % 100}",
        ts,
        1,
        float(random.randint(1, 4)),
        round(random.uniform(0.5, 30.0), 2),
        random.randint(1, 265),
        random.randint(1, 265),
        round(random.uniform(5.0, 100.0), 2),
        round(random.uniform(0.0, 20.0), 2),
        round(random.uniform(5.0, 120.0), 2),
        random.randint(1, 4)
    ))
    futures.append(future)

    if len(futures) >= 500:
        for f in futures:
            f.result()
        futures = []
        done = i + 1
        if done % 100000 == 0:
            elapsed = time.time() - start
            print(f"    Cassandra: {done:,} writes done ({elapsed:.1f}s)")

for f in futures:
    f.result()

cass_write_time = time.time() - start
print(f"  Cassandra done: {num_writes:,} writes in {cass_write_time:.1f}s ({num_writes/cass_write_time:,.0f} writes/sec)")
print()

# mongodb sequential single inserts
print("  Running MongoDB writes...")
start = time.time()

for i in range(num_writes):
    ts = base_time + timedelta(seconds=i)
    mdb.trips.insert_one({
        "sensor_id": f"sensor_{i % 100}",
        "timestamp": ts,
        "VendorID": 1,
        "passenger_count": float(random.randint(1, 4)),
        "trip_distance": round(random.uniform(0.5, 30.0), 2),
        "PULocationID": random.randint(1, 265),
        "DOLocationID": random.randint(1, 265),
        "fare_amount": round(random.uniform(5.0, 100.0), 2),
        "tip_amount": round(random.uniform(0.0, 20.0), 2),
        "total_amount": round(random.uniform(5.0, 120.0), 2),
        "payment_type": random.randint(1, 4)
    })
    if (i + 1) % 100000 == 0:
        elapsed = time.time() - start
        print(f"    MongoDB: {i+1:,} writes done ({elapsed:.1f}s)")

mongo_write_time = time.time() - start
print(f"  MongoDB done: {num_writes:,} writes in {mongo_write_time:.1f}s ({num_writes/mongo_write_time:,.0f} writes/sec)")

print()
print("Demo 1 Results:")
print(f"  Cassandra: {cass_write_time:.1f}s ({num_writes/cass_write_time:,.0f} writes/sec)")
print(f"  MongoDB:   {mongo_write_time:.1f}s ({num_writes/mongo_write_time:,.0f} writes/sec)")
winner1 = "Cassandra" if cass_write_time < mongo_write_time else "MongoDB"
print(f"  Winner: {winner1}")
print()


# -- DEMO 2: bulk deletion --
# deleting all trips with low fares from the base data

print("DEMO 2: Bulk Data Deletion")
print("Deleting all trips where fare < $10 from both databases")
print()

# count before
mongo_to_delete = mdb.trips.count_documents({"fare_amount": {"$lt": 10}})
print(f"  MongoDB records matching condition: {mongo_to_delete:,}")

# cassandra delete - have to read keys first then delete one by one
print("  Running Cassandra deletes...")
start = time.time()
cass_deleted = 0
sensor_ids = [f"taxi_{i}" for i in range(1, 266)]

for sid in sensor_ids:
    try:
        rows = list(cass.execute(
            f"SELECT timestamp, fare_amount FROM trips WHERE sensor_id = '{sid}'"
        ))
        for row in rows:
            if row.fare_amount is not None and row.fare_amount < 10:
                cass.execute(
                    f"DELETE FROM trips WHERE sensor_id = '{sid}' AND timestamp = '{row.timestamp}'"
                )
                cass_deleted += 1
    except:
        pass
    if cass_deleted % 5000 == 0 and cass_deleted > 0:
        print(f"    Cassandra: {cass_deleted:,} rows deleted ({time.time()-start:.1f}s)")

cass_delete_time = time.time() - start
print(f"  Cassandra done: {cass_deleted:,} rows deleted in {cass_delete_time:.1f}s")
print()

# mongodb delete
print("  Running MongoDB deletes...")
start = time.time()
result = mdb.trips.delete_many({"fare_amount": {"$lt": 10}})
mongo_delete_time = time.time() - start
print(f"  MongoDB done: {result.deleted_count:,} rows deleted in {mongo_delete_time:.1f}s")

print()
print("Demo 2 Results:")
print(f"  Cassandra: {cass_deleted:,} rows in {cass_delete_time:.1f}s")
print(f"  MongoDB:   {result.deleted_count:,} rows in {mongo_delete_time:.1f}s")
print(f"  Note: Cassandra can't do range deletes on non-key columns, needs row-by-row deletion")
print(f"  Note: MongoDB delete_many handles it in one command")
print()


# -- DEMO 3: complex aggregation --
# aggregate across ALL pickup locations, not just a few

print("DEMO 3: Complex Aggregation")
print("Average fare, total revenue, trip count across ALL pickup locations")
print()

# cassandra - fetch from many partitions and aggregate in python
print("  Running Cassandra aggregation...")
start = time.time()
cass_agg = {}
all_locations = [f"taxi_{i}" for i in range(1, 266)]

for sid in all_locations:
    try:
        rows = list(cass.execute(
            f"SELECT fare_amount, total_amount FROM trips WHERE sensor_id = '{sid}'"
        ))
        if rows:
            loc = int(sid.split("_")[1])
            fares = [r.fare_amount for r in rows if r.fare_amount is not None]
            totals = [r.total_amount for r in rows if r.total_amount is not None]
            if fares:
                cass_agg[loc] = {
                    'avg_fare': sum(fares)/len(fares),
                    'total_revenue': sum(totals),
                    'trip_count': len(rows)
                }
    except:
        pass

cass_agg_time = time.time() - start

# sort by revenue and show top 5
sorted_cass = sorted(cass_agg.items(), key=lambda x: x[1]['total_revenue'], reverse=True)
print(f"  Cassandra done: aggregated {len(cass_agg)} locations in {cass_agg_time:.1f}s")
for loc, data in sorted_cass[:5]:
    print(f"    Location {loc}: avg fare=${data['avg_fare']:.2f}, revenue=${data['total_revenue']:,.2f}, trips={data['trip_count']:,}")
print()

# mongodb - native aggregation pipeline across all locations
print("  Running MongoDB aggregation...")
start = time.time()
pipeline = [
    {"$group": {
        "_id": "$PULocationID",
        "avg_fare": {"$avg": "$fare_amount"},
        "total_revenue": {"$sum": "$total_amount"},
        "trip_count": {"$sum": 1}
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 265}
]
mongo_agg = list(mdb.trips.aggregate(pipeline))
mongo_agg_time = time.time() - start

print(f"  MongoDB done: aggregated {len(mongo_agg)} locations in {mongo_agg_time:.1f}s")
for r in mongo_agg[:5]:
    print(f"    Location {r['_id']}: avg fare=${r['avg_fare']:.2f}, revenue=${r['total_revenue']:,.2f}, trips={r['trip_count']:,}")

print()
print("Demo 3 Results:")
print(f"  Cassandra: {cass_agg_time:.1f}s (fetched data from each partition, aggregated in Python)")
print(f"  MongoDB:   {mongo_agg_time:.1f}s (native aggregation pipeline)")
print()


# -- DEMO 4: schema flexibility --
# adding new fields to existing records based on a condition

print("DEMO 4: Schema Flexibility")
print("Adding surge_pricing and peak_hour fields to matching records")
print()

# mongodb - add multiple new fields with conditions, no schema change
print("  Running MongoDB schema update...")
start = time.time()

# add surge_pricing to high fare trips
result1 = mdb.trips.update_many(
    {"fare_amount": {"$gt": 50}},
    {"$set": {"surge_pricing": True, "surge_multiplier": 1.5}}
)

# add peak_hour flag to trips during rush hour (7-9am and 5-7pm)
result2 = mdb.trips.update_many(
    {"$or": [
        {"tpep_pickup_datetime": {"$gte": datetime(2024, 1, 1, 7, 0), "$lt": datetime(2024, 1, 1, 9, 0)}},
    ]},
    {"$set": {"peak_hour": True}}
)

mongo_flex_time = time.time() - start
print(f"  MongoDB: added surge_pricing to {result1.modified_count:,} records")
print(f"  MongoDB: added peak_hour to {result2.modified_count:,} records")
print(f"  MongoDB: total time {mongo_flex_time:.2f}s, no schema change needed")

# verify
sample = mdb.trips.find_one({"surge_pricing": True})
if sample:
    print(f"  MongoDB: verified, new fields are queryable immediately")
print()

# cassandra - needs ALTER TABLE for each new column
print("  Running Cassandra schema update...")
start = time.time()
try:
    cass.execute("ALTER TABLE trips ADD surge_pricing BOOLEAN")
    cass.execute("ALTER TABLE trips ADD surge_multiplier DOUBLE")
    cass.execute("ALTER TABLE trips ADD peak_hour BOOLEAN")
except:
    pass
cass_flex_time = time.time() - start

print(f"  Cassandra: ALTER TABLE needed for each new column ({cass_flex_time:.2f}s)")
print(f"  Cassandra: can't do conditional updates like 'UPDATE WHERE fare > 50'")
print(f"  Cassandra: to update matching rows, you'd have to:")
print(f"    1. Read every row from each partition")
print(f"    2. Check the condition in Python")
print(f"    3. Update matching rows one by one")
print(f"  Cassandra: this would take significantly longer than MongoDB's approach")

print()
print("Demo 4 Results:")
print(f"  MongoDB:   added fields in {mongo_flex_time:.2f}s, no schema changes, conditional updates work")
print(f"  Cassandra: schema changes required, conditional updates not supported in CQL")
print()


# -- final summary --
print("FINAL SUMMARY")
print(f"Dataset: NYC Yellow Taxi, Jan-Apr 2024 ({len(df):,} total rows)")
print(f"Base data loaded: 1,000,000 rows into each database")
print(f"Platform: Google Cloud Platform VM (e2-standard-4, Ubuntu 22.04)")
print()
print(f"Demo 1 - Sequential Writes ({num_writes:,} records):")
print(f"  Cassandra: {cass_write_time:.1f}s ({num_writes/cass_write_time:,.0f} writes/sec)")
print(f"  MongoDB:   {mongo_write_time:.1f}s ({num_writes/mongo_write_time:,.0f} writes/sec)")
print()
print(f"Demo 2 - Bulk Deletion:")
print(f"  Cassandra: {cass_deleted:,} rows in {cass_delete_time:.1f}s (row by row)")
print(f"  MongoDB:   {result.deleted_count:,} rows in {mongo_delete_time:.1f}s (delete_many)")
print()
print(f"Demo 3 - Aggregation (all locations):")
print(f"  Cassandra: {cass_agg_time:.1f}s (manual in Python)")
print(f"  MongoDB:   {mongo_agg_time:.1f}s (native pipeline)")
print()
print(f"Demo 4 - Schema Flexibility:")
print(f"  MongoDB:   flexible, no schema changes needed")
print(f"  Cassandra: rigid, ALTER TABLE required")
print()
print("Conclusion:")
print("  Cassandra: best for high-throughput sequential writes and distributed systems")
print("  MongoDB: best for complex queries, aggregation, and flexible schemas")

# save results
all_results = {
    'dataset': f'NYC Yellow Taxi Jan-Apr 2024, {len(df):,} total',
    'base_data': '1,000,000 rows in each database',
    'platform': 'GCP VM e2-standard-4, Ubuntu 22.04',
    'demo1_writes': num_writes,
    'demo1_cassandra_sec': cass_write_time,
    'demo1_mongodb_sec': mongo_write_time,
    'demo2_cassandra_deleted': cass_deleted,
    'demo2_cassandra_sec': cass_delete_time,
    'demo2_mongodb_deleted': result.deleted_count,
    'demo2_mongodb_sec': mongo_delete_time,
    'demo3_cassandra_sec': cass_agg_time,
    'demo3_mongodb_sec': mongo_agg_time,
    'demo4_mongodb_surge_updated': result1.modified_count,
    'demo4_mongodb_sec': mongo_flex_time,
}
with open('results.json', 'w') as f:
    json.dump(all_results, f, indent=2)

print("\nResults saved to results.json")

cluster.shutdown()
mongo.close()
