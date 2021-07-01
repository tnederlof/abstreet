use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Deserializer};

use geom::LonLat;
use map_model::raw::{RawBusRoute, RawBusShape, RawBusStop, RawBusStopTime, RawBusTrip, RawMap};

pub fn import_gtfs(map: &mut RawMap, paths: &Vec<String>) -> Result<Vec<RawBusRoute>> {
    let all_raw_bus_routes: Vec<RawBusRoute> = Vec::new();
    // parse raw gtfs text files into record
    for path in paths {
        // parse routes
        let routes_full_path = format!("{}{}", path, "routes.txt");
        let mut route_records = Vec::new();
        for rec in csv::Reader::from_reader(std::fs::File::open(routes_full_path)?).deserialize() {
            let rec: RouteRecord = rec?;
            route_records.push(rec);
        }
        // parse stops
        let stops_full_path = format!("{}{}", path, "stops.txt");
        let mut stop_records = Vec::new();
        for rec in csv::Reader::from_reader(std::fs::File::open(stops_full_path)?).deserialize() {
            let rec: StopRecord = rec?;
            stop_records.push(rec);
        }
        // parse stop_times
        let stop_times_full_path = format!("{}{}", path, "stop_times.txt");
        let mut stop_times_records = Vec::new();
        for rec in
            csv::Reader::from_reader(std::fs::File::open(stop_times_full_path)?).deserialize()
        {
            let rec: StopTimesRecord = rec?;
            stop_times_records.push(rec);
        }
        // parse trips
        let trips_full_path = format!("{}{}", path, "trips.txt");
        let mut trip_records = Vec::new();
        for rec in csv::Reader::from_reader(std::fs::File::open(trips_full_path)?).deserialize() {
            let rec: TripRecord = rec?;
            trip_records.push(rec);
        }
        // parse shapes
        let shapes_path = format!("{}{}", path, "shapes.txt");
        let mut shape_records = Vec::new();
        for rec in csv::Reader::from_reader(std::fs::File::open(shapes_path)?).deserialize() {
            let rec: ShapeRecord = rec?;
            shape_records.push(rec);
        }

        let mut raw_bus_routes: Vec<RawBusRoute> = Vec::new();
        for route_rec in route_records {
            // find the service_id with the > n and only keep trips in it
            let mut service_counts = BTreeMap::new();
            for trip in trip_records
                .iter()
                .filter(|t| t.route_id == route_rec.route_id)
                .map(|x| &x.service_id)
            {
                *service_counts.entry(trip).or_insert(0) += 1;
            }
            let service_id: String = service_counts
                .into_iter()
                .max_by_key(|&(_, count)| count)
                .map(|(val, _)| val)
                .unwrap()
                .clone();
            let trips_to_use: Vec<&TripRecord> = trip_records
                .iter()
                .filter(|t| t.service_id == service_id)
                .collect();
            // find the trip with the most stops and use those stops to link to route
            let mut trip_counts = BTreeMap::new();
            for trip in trips_to_use.iter() {
                let mut trip_stops = Vec::new();
                for stop_time in stop_times_records
                    .iter()
                    .filter(|st| trip.trip_id == st.trip_id)
                {
                    for stop in stop_records
                        .iter()
                        .filter(|s| s.stop_id == stop_time.stop_id)
                    {
                        trip_stops.push(stop);
                    }
                }
                *trip_counts.entry(trip.trip_id).or_insert(0) += trip_stops.len();
            }
            let trip_id: String = trip_counts
                .into_iter()
                .max_by_key(|&(_, count)| count)
                .map(|(val, _)| val)
                .unwrap();
            // create a vector of valid stops
            let mut all_stops: Vec<StopRecord> = Vec::new();
            for stop_time in stop_times_records.iter().filter(|st| st.trip_id == trip_id) {
                for stop in stop_records
                    .into_iter()
                    .filter(|s| s.stop_id == stop_time.stop_id)
                {
                    all_stops.push(stop);
                }
            }
            let all_stops_distinct: Vec<StopRecord> = all_stops.sort_by_key(|s| s.stop_id);

            // create the raw data
            raw_bus_routes.push(RawBusRoute {
                id: route_rec.route_id,
                long_name: route_rec.route_long_name,
                short_name: route_rec.route_short_name,
                description: route_rec.route_desc,
                stops: all_stops_distinct
                    .iter()
                    .map(|s| RawBusStop {
                        id: s.stop_id,
                        code: s.stop_code,
                        name: s.stop_name,
                        description: s.stop_desc,
                        position: LonLat::new(s.stop_lon, s.stop_lat).to_pt(&map.gps_bounds),
                    })
                    .collect(),
                trips: trips_to_use
                    .iter()
                    .map(|t| RawBusTrip {
                        service_id: t.service_id,
                        id: t.trip_id,
                        trip_headsign: t.trip_headsign,
                        direction_id: t.direction_id,
                        shapes: shape_records
                            .iter()
                            .filter(|s| s.shape_id == t.shape_id)
                            .map(|s| RawBusShape {
                                position: LonLat::new(s.shape_pt_lon, s.shape_pt_lat)
                                    .to_pt(&map.gps_bounds),
                                sequence: s.shape_pt_sequence,
                            })
                            .collect(),
                        stop_times: stop_times_records
                            .iter()
                            .filter(|st| st.trip_id == t.trip_id)
                            .map(|st| RawBusStopTime {
                                arrival_time: st.arrival_time,
                                departure_time: st.departure_time,
                                stop: stop_records
                                    .iter()
                                    .filter(|s| s.stop_id == st.stop_id)
                                    .flat_map(|s| RawBusStop {
                                        id: s.stop_id,
                                        code: s.stop_code,
                                        name: s.stop_name,
                                        description: s.stop_desc,
                                        position: LonLat::new(s.stop_lon, s.stop_lat)
                                            .to_pt(&map.gps_bounds),
                                    })
                                    .collect(),
                                stop_sequence: st.stop_sequence,
                                pickup_type: st.pickup_type,
                                drop_off_type: st.drop_off_type,
                            })
                            .collect(),
                    })
                    .collect(),
            });
        }
        all_raw_bus_routes.extend(raw_bus_routes)
    }
    Ok(all_raw_bus_routes)
}

#[derive(Clone, Debug, Deserialize)]
struct RouteRecord {
    route_id: String,
    route_short_name: String,
    route_long_name: String,
    route_desc: String,
    route_type: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct StopRecord {
    stop_id: String,
    stop_code: String,
    stop_name: String,
    stop_desc: String,
    #[serde(deserialize_with = "parse_coord")]
    stop_lat: f64,
    #[serde(deserialize_with = "parse_coord")]
    stop_lon: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct TripRecord {
    route_id: String,
    service_id: String,
    trip_id: String,
    trip_headsign: String,
    direction_id: usize,
    block_id: String,
    shape_id: String,
    short_trip_no: String,
}

#[derive(Clone, Debug, Deserialize)]
struct StopTimesRecord {
    trip_id: String,
    arrival_time: String,
    departure_time: String,
    stop_id: String,
    stop_sequence: usize,
    pickup_type: usize,
    drop_off_type: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct ShapeRecord {
    shape_id: String,
    #[serde(deserialize_with = "parse_coord")]
    shape_pt_lat: f64,
    #[serde(deserialize_with = "parse_coord")]
    shape_pt_lon: f64,
    shape_pt_sequence: usize,
}

fn parse_coord<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let value = <String>::deserialize(d)?;
    value
        .parse::<f64>()
        .map_err(|_err| serde::de::Error::custom(format!("bad point {}", value)))
}
