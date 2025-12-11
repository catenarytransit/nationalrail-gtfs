use anyhow::{Context, Result};
use csv::Writer;
use lonlat_bng::convert_osgb36_to_ll;
use osmpbfreader::{OsmPbfReader, objects::OsmObj};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Cursor, Read};
use zip::ZipArchive;

// --- Configuration ---
const AUTH_URL: &str = "https://opendata.nationalrail.co.uk/authenticate";
const TIMETABLE_URL: &str = "https://opendata.nationalrail.co.uk/api/staticfeeds/3.0/timetable";
const FARES_URL: &str = "https://opendata.nationalrail.co.uk/api/staticfeeds/2.0/fares";
const OSM_CRS_URL: &str = "https://github.com/catenarytransit/osm-filter/releases/download/latest/crs-networkrail.osm.pbf";

// Placeholder for trip_id during stop collection
const PLACEHOLDER_TRIP_ID: &str = "PLACEHOLDER";

// --- Data Structures ---

#[derive(Deserialize)]
struct AuthResponse {
    token: String,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq, Hash)]
struct Agency {
    agency_id: String,
    agency_name: String,
    agency_url: String,
    agency_timezone: String,
}

#[derive(Debug, Serialize)]
struct Stop {
    stop_id: String,
    stop_name: String,
    stop_lat: f64,
    stop_lon: f64,
}

#[derive(Debug, Serialize)]
struct Route {
    route_id: String,
    agency_id: String,
    route_short_name: String,
    route_long_name: String,
    route_type: u8,
    route_color: String,
    route_text_color: String,
}

#[derive(Debug, Serialize)]
struct Trip {
    route_id: String,
    service_id: String,
    trip_id: String,
    trip_headsign: String,
    #[serde(rename = "trip_short_name")]
    trip_short_name: String,
}

#[derive(Debug, Serialize, Clone)]
struct StopTime {
    trip_id: String,
    arrival_time: String,
    departure_time: String,
    stop_id: String,
    stop_sequence: u32,
}

#[derive(Debug, Serialize)]
struct Calendar {
    service_id: String,
    monday: u8,
    tuesday: u8,
    wednesday: u8,
    thursday: u8,
    friday: u8,
    saturday: u8,
    sunday: u8,
    start_date: String,
    end_date: String,
}

#[derive(Debug, Serialize)]
struct Association {
    base_uid: String,
    assoc_uid: String,
    start_date: String,
    end_date: String,
    days_run: String,
    category: String,
    location: String,
    assoc_type: String,
    stp_indicator: String,
}

struct ParsedStation {
    tiploc: String,
    name: String,
    lat: f64,
    lon: f64,
}

struct TripState {
    uid: String,
    date_start: String,
    date_end: String,
    days_run: String,
    stp_ind: String,
    atoc_code: String,
    train_identity: String,
    origin_name: String,
    dest_name: String,
    stops: Vec<StopTime>,
}

// Signature for identifying identical trip patterns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TripSignature {
    route_id: String,
    stop_pattern: Vec<(String, String, String)>, // (stop_id, arrival_time, departure_time)
    headsign: String,
    train_identity: String,
}

// Composite signature for trip+service combination (for deduplication)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TripServiceSignature {
    trip_sig: TripSignature,
    service_id: String,
}

// Signature for identifying identical calendar entries
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CalendarSignature {
    monday: u8,
    tuesday: u8,
    wednesday: u8,
    thursday: u8,
    friday: u8,
    saturday: u8,
    sunday: u8,
    start_date: String,
    end_date: String,
}

// --- Authentication ---

fn authenticate(username: &str, password: &str) -> Result<String> {
    println!("Authenticating with NRDP...");
    let client = reqwest::blocking::Client::new();
    let params = [("username", username), ("password", password)];

    let res = client
        .post(AUTH_URL)
        .form(&params)
        .send()
        .context("Failed to send authentication request")?;

    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().unwrap_or_default();
        anyhow::bail!("Authentication failed ({}): {}", status, text);
    }

    let auth_data: AuthResponse = res.json().context("Failed to parse auth JSON")?;
    println!("Authentication successful.");
    Ok(auth_data.token)
}

// --- Main Execution ---

fn main() -> Result<()> {
    let username = std::env::var("NR_USERNAME").expect("NR_USERNAME must be set");
    let password = std::env::var("NR_PASSWORD").expect("NR_PASSWORD must be set");

    let output_dir = "./gtfs_output";
    fs::create_dir_all(output_dir)?;

    // 1. Download and Parse OSM CRS Data
    println!("Downloading OSM CRS Data from {}...", OSM_CRS_URL);
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;

    // We save PBF to disk temporarily because OsmPbfReader prefers a File or Seekable stream
    let pbf_path = format!("{}/stations.pbf", output_dir);
    let mut pbf_file = File::create(&pbf_path)?;
    let mut pbf_resp = client.get(OSM_CRS_URL).send()?;
    pbf_resp.copy_to(&mut pbf_file)?;

    println!("Parsing OSM PBF...");
    let osm_crs_map = parse_osm_crs(&pbf_path)?;
    println!("Loaded {} stations from OSM.", osm_crs_map.len());

    // 2. Authenticate
    let token = authenticate(&username, &password)?;

    // 3. Download and Parse Fares Feed (For TOC Names)
    println!("Downloading Fares Feed from {}...", FARES_URL);
    let fares_resp = client
        .get(FARES_URL)
        .header("X-Auth-Token", &token)
        .send()
        .context("Failed to download fares feed")?
        .bytes()?;

    let mut fares_archive = ZipArchive::new(Cursor::new(fares_resp))?;
    let mut toc_map: HashMap<String, String> = HashMap::new();

    for i in 0..fares_archive.len() {
        let mut file = fares_archive.by_index(i)?;
        if file.name().ends_with(".TOC") {
            println!("Processing Fares TOC File: {}", file.name());
            parse_fares_toc(&mut file, &mut toc_map)?;
        }
    }

    // 4. Download and Parse Timetable Feed
    println!("Downloading Timetable Feed from {}...", TIMETABLE_URL);
    let tt_resp = client
        .get(TIMETABLE_URL)
        .header("X-Auth-Token", &token)
        .send()
        .context("Failed to download timetable feed")?
        .bytes()?;

    let mut tt_archive = ZipArchive::new(Cursor::new(tt_resp))?;
    let mut tiploc_map: HashMap<String, ParsedStation> = HashMap::new();

    // 4a. Process Stations (MSN)
    for i in 0..tt_archive.len() {
        let mut file = tt_archive.by_index(i)?;
        if file.name().ends_with(".MSN") {
            println!("Processing Station File: {}", file.name());
            parse_msn(&mut file, &mut tiploc_map, &osm_crs_map)?;
        }
    }

    // 5. Initialize CSV Writers
    let mut stops_writer = Writer::from_path(format!("{}/stops.txt", output_dir))?;
    let mut trips_writer = Writer::from_path(format!("{}/trips.txt", output_dir))?;
    let mut st_writer = Writer::from_path(format!("{}/stop_times.txt", output_dir))?;
    let mut cal_writer = Writer::from_path(format!("{}/calendar.txt", output_dir))?;
    let mut routes_writer = Writer::from_path(format!("{}/routes.txt", output_dir))?;
    let mut agency_writer = Writer::from_path(format!("{}/agency.txt", output_dir))?;
    let mut assoc_writer = Writer::from_path(format!("{}/associations.txt", output_dir))?;

    // Write Stops
    for station in tiploc_map.values() {
        stops_writer.serialize(Stop {
            stop_id: station.tiploc.clone(),
            stop_name: station.name.clone(),
            stop_lat: station.lat,
            stop_lon: station.lon,
        })?;
    }

    let mut agencies: HashSet<Agency> = HashSet::new();
    let mut routes: HashMap<String, Route> = HashMap::new();
    
    // Maps for consolidating identical trips and calendars
    let mut trip_service_to_id: HashMap<TripServiceSignature, String> = HashMap::new();
    let mut uid_usage_count: HashMap<String, u32> = HashMap::new();
    let mut calendar_signature_to_id: HashMap<CalendarSignature, String> = HashMap::new();
    let mut service_counter = 0u32;

    // 4b. Process Timetable (MCA)
    for i in 0..tt_archive.len() {
        let mut file = tt_archive.by_index(i)?;
        if file.name().ends_with(".MCA") {
            println!("Processing Timetable File: {}", file.name());
            parse_mca(
                &mut file,
                &mut trips_writer,
                &mut st_writer,
                &mut cal_writer,
                &mut assoc_writer,
                &tiploc_map,
                &mut agencies,
                &mut routes,
                &toc_map,
                &mut trip_service_to_id,
                &mut uid_usage_count,
                &mut calendar_signature_to_id,
                &mut service_counter,
            )?;
        }
    }

    // Print consolidation statistics
    println!("Consolidation Summary:");
    println!("  Unique trip+service combinations: {}", trip_service_to_id.len());
    println!("  Unique service calendars: {}", service_counter);
    println!("  Total routes: {}", routes.len());
    println!("  Total agencies: {}", agencies.len());

    // Write aggregated Agencies and Routes
    for agency in agencies {
        agency_writer.serialize(agency)?;
    }
    for route in routes.values() {
        routes_writer.serialize(route)?;
    }

    println!("Conversion complete.");
    Ok(())
}

// --- Parsing Logic ---

/// Parse OSM PBF to get CRS -> Lat/Lon map
fn parse_osm_crs(path: &str) -> Result<HashMap<String, (f64, f64)>> {
    let file = File::open(path)?;
    let mut reader = OsmPbfReader::new(file);
    let mut map = HashMap::new();

    for obj in reader.iter().flatten() {
        if let OsmObj::Node(node) = obj {
            // Look for ref:crs tag
            if let Some(crs) = node.tags.get("ref:crs") {
                // Some CRS might be comma separated or slight variations, taking direct 3-char match usually
                // The provided PBF is filtered for CRS, so we trust it.
                // We store the lat/lon directly from the node.
                map.insert(crs.to_string(), (node.lat(), node.lon()));
            }
        }
    }
    Ok(map)
}

fn parse_fares_toc<R: Read>(reader: &mut R, map: &mut HashMap<String, String>) -> Result<()> {
    let buf_reader = BufReader::new(reader);
    for line in buf_reader.lines().flatten() {
        if line.starts_with('T') {
            let id = line.get(1..3).unwrap_or("").trim().to_string();
            let name = line.get(3..33).unwrap_or("").trim().to_string();
            if !id.is_empty() && !name.is_empty() {
                map.insert(id, name);
            }
        }
    }
    Ok(())
}

/// Parse Master Station Names
/// Prioritizes OSM coordinates if CRS matches, otherwise falls back to OSGB36 conversion
fn parse_msn<R: Read>(
    reader: &mut R,
    map: &mut HashMap<String, ParsedStation>,
    osm_lookup: &HashMap<String, (f64, f64)>,
) -> Result<()> {
    let buf_reader = BufReader::new(reader);
    for line in buf_reader.lines().flatten() {
        if line.starts_with('A') {
            // RSPS5046 Page 33
            // Name: 6-31 (0-based 5..31)
            let name = line.get(5..31).unwrap_or("").trim().to_string();
            // TIPLOC: 37-43 (0-based 36..43)
            let tiploc = line.get(36..43).unwrap_or("").trim().to_string();
            // CRS Code: 50-52 (0-based 49..52)
            let crs = line.get(49..52).unwrap_or("").trim().to_string();

            // Easting: 53-57 (52..57)
            let easting_str = line.get(52..57).unwrap_or("0");
            // Northing: 59-63 (58..63)
            let northing_str = line.get(58..63).unwrap_or("0");

            let (lat, lon) = if let Some(coords) = osm_lookup.get(&crs) {
                // 1. Priority: OSM Match via CRS
                *coords
            } else {
                // 2. Fallback: OSGB36 Conversion
                let easting = easting_str.trim().parse::<f64>().unwrap_or(0.0) * 100.0;
                let northing = northing_str.trim().parse::<f64>().unwrap_or(0.0) * 100.0;
                match convert_osgb36_to_ll(easting, northing) {
                    Ok(coords) => coords,
                    Err(_) => (0.0, 0.0),
                }
            };

            if !tiploc.is_empty() {
                map.insert(
                    tiploc.clone(),
                    ParsedStation {
                        tiploc,
                        name,
                        lat,
                        lon,
                    },
                );
            }
        }
    }
    Ok(())
}

fn parse_mca<R: Read>(
    reader: &mut R,
    trips_w: &mut Writer<File>,
    st_w: &mut Writer<File>,
    cal_w: &mut Writer<File>,
    assoc_w: &mut Writer<File>,
    tiploc_map: &HashMap<String, ParsedStation>,
    agencies_set: &mut HashSet<Agency>,
    routes_map: &mut HashMap<String, Route>,
    toc_lookup: &HashMap<String, String>,
    trip_service_to_id: &mut HashMap<TripServiceSignature, String>,
    uid_usage_count: &mut HashMap<String, u32>,
    calendar_signature_to_id: &mut HashMap<CalendarSignature, String>,
    service_counter: &mut u32,
) -> Result<()> {
    let buf_reader = BufReader::new(reader);
    let mut current_trip: Option<TripState> = None;
    let mut seq_counter = 0;

    for line in buf_reader.lines().flatten() {
        if line.len() < 2 {
            continue;
        }
        let record_type = &line[0..2];

        match record_type {
            "BS" => {
                let uid = line.get(3..9).unwrap_or("").to_string();
                let d_start = line.get(9..15).unwrap_or("");
                let d_end = line.get(15..21).unwrap_or("");
                let days = line.get(21..28).unwrap_or("0000000");
                let train_id = line.get(32..36).unwrap_or("").trim().to_string();
                let stp = line.get(79..80).unwrap_or("P");

                if stp == "C" {
                    current_trip = None;
                    continue;
                }

                current_trip = Some(TripState {
                    uid: uid.clone(),
                    date_start: d_start.to_string(),
                    date_end: d_end.to_string(),
                    days_run: days.to_string(),
                    stp_ind: stp.to_string(),
                    atoc_code: "NR".to_string(),
                    train_identity: train_id,
                    origin_name: String::new(),
                    dest_name: String::new(),
                    stops: Vec::new(),
                });

                seq_counter = 1;
            }
            "BX" => {
                if let Some(trip) = &mut current_trip {
                    let atoc = line.get(11..13).unwrap_or("NR").trim().to_string();
                    if !atoc.is_empty() {
                        trip.atoc_code = atoc;
                    }
                }
            }
            "LO" => {
                if let Some(trip) = &mut current_trip {
                    let tiploc = line.get(2..9).unwrap_or("").trim();
                    let dep_sched = format_time(line.get(10..15).unwrap_or("00000"));
                    let _dep_pub = line.get(15..19).unwrap_or("0000");

                    // Filter operational stops if necessary, currently strictly filtering on MSN existence
                    if let Some(station) = tiploc_map.get(tiploc) {
                        trip.origin_name = station.name.clone();
                        trip.stops.push(StopTime {
                            trip_id: PLACEHOLDER_TRIP_ID.to_string(),
                            arrival_time: dep_sched.clone(),
                            departure_time: dep_sched,
                            stop_id: tiploc.to_string(),
                            stop_sequence: seq_counter,
                        });
                        seq_counter += 1;
                    }
                }
            }
            "LI" => {
                if let Some(trip) = &mut current_trip {
                    let tiploc = line.get(2..9).unwrap_or("").trim();
                    let arr_sched = format_time(line.get(10..15).unwrap_or("00000"));
                    let dep_sched = format_time(line.get(15..20).unwrap_or("00000"));

                    let pub_arr = line.get(25..29).unwrap_or("0000");
                    let pub_dep = line.get(29..33).unwrap_or("0000");

                    // Filter operational stops: Must have public times AND exist in station map
                    if pub_arr == "0000" && pub_dep == "0000" {
                        continue;
                    }

                    if tiploc_map.contains_key(tiploc) {
                        trip.stops.push(StopTime {
                            trip_id: PLACEHOLDER_TRIP_ID.to_string(),
                            arrival_time: arr_sched,
                            departure_time: dep_sched,
                            stop_id: tiploc.to_string(),
                            stop_sequence: seq_counter,
                        });
                        seq_counter += 1;
                    }
                }
            }
            "LT" => {
                if let Some(trip) = &mut current_trip {
                    let tiploc = line.get(2..9).unwrap_or("").trim();
                    let arr_sched = format_time(line.get(10..15).unwrap_or("00000"));

                    if let Some(station) = tiploc_map.get(tiploc) {
                        trip.dest_name = station.name.clone();
                        
                        // Use placeholder trip_id for building the stop pattern
                        trip.stops.push(StopTime {
                            trip_id: PLACEHOLDER_TRIP_ID.to_string(),
                            arrival_time: arr_sched.clone(),
                            departure_time: arr_sched,
                            stop_id: tiploc.to_string(),
                            stop_sequence: seq_counter,
                        });

                        // Routes & Agencies
                        let agency_name = toc_lookup
                            .get(&trip.atoc_code)
                            .cloned()
                            .unwrap_or_else(|| format!("National Rail ({})", trip.atoc_code));

                        let mut route_id = format!("{}_{}", trip.atoc_code, trip.origin_name);
                        let mut route_name = format!("{} to {}", trip.origin_name, trip.dest_name);
                        let mut route_short_name = trip.atoc_code.clone();
                        let mut route_color = "".to_string(); // Default (or undefined)
                        let mut route_text_color = "000000".to_string(); // Default Black

                        if trip.atoc_code == "LO" {
                            let (name, id, color) = get_lo_line_details(&trip.stops, tiploc_map);
                            if !name.is_empty() {
                                route_id = id;
                                route_name = name;
                                route_short_name = "LO".to_string();
                                route_color = color;
                                route_text_color = "FFFFFF".to_string();
                            }
                        }

                        agencies_set.insert(Agency {
                            agency_id: trip.atoc_code.clone(),
                            agency_name: agency_name,
                            agency_url: "http://www.nationalrail.co.uk".to_string(),
                            agency_timezone: "Europe/London".to_string(),
                        });

                        routes_map.entry(route_id.clone()).or_insert(Route {
                            route_id: route_id.clone(),
                            agency_id: trip.atoc_code.clone(),
                            route_short_name,
                            route_long_name: route_name,
                            route_type: 2,
                            route_color,
                            route_text_color,
                        });

                        // Create calendar signature and get or create service_id
                        let d_vec: Vec<u8> = trip.days_run.chars().map(|c| if c == '1' { 1 } else { 0 }).collect();
                        let cal_sig = CalendarSignature {
                            monday: *d_vec.get(0).unwrap_or(&0),
                            tuesday: *d_vec.get(1).unwrap_or(&0),
                            wednesday: *d_vec.get(2).unwrap_or(&0),
                            thursday: *d_vec.get(3).unwrap_or(&0),
                            friday: *d_vec.get(4).unwrap_or(&0),
                            saturday: *d_vec.get(5).unwrap_or(&0),
                            sunday: *d_vec.get(6).unwrap_or(&0),
                            start_date: format!("20{}", trip.date_start),
                            end_date: format!("20{}", trip.date_end),
                        };

                        // Check if we've seen this calendar signature before
                        let service_id = if let Some(existing_id) = calendar_signature_to_id.get(&cal_sig) {
                            existing_id.clone()
                        } else {
                            let new_id = format!("SVC{}", service_counter);
                            *service_counter += 1;
                            
                            // Write the calendar entry for this new service
                            cal_w.serialize(Calendar {
                                service_id: new_id.clone(),
                                monday: cal_sig.monday,
                                tuesday: cal_sig.tuesday,
                                wednesday: cal_sig.wednesday,
                                thursday: cal_sig.thursday,
                                friday: cal_sig.friday,
                                saturday: cal_sig.saturday,
                                sunday: cal_sig.sunday,
                                start_date: cal_sig.start_date.clone(),
                                end_date: cal_sig.end_date.clone(),
                            })?;
                            
                            calendar_signature_to_id.insert(cal_sig, new_id.clone());
                            new_id
                        };

                        // Create trip signature (with normalized stop pattern)
                        let stop_pattern: Vec<(String, String, String)> = trip.stops.iter()
                            .map(|st| (st.stop_id.clone(), st.arrival_time.clone(), st.departure_time.clone()))
                            .collect();
                        
                        let trip_sig = TripSignature {
                            route_id: route_id.clone(),
                            stop_pattern,
                            headsign: trip.dest_name.clone(),
                            train_identity: trip.train_identity.clone(),
                        };

                        // Create composite signature combining trip pattern and service
                        let trip_service_sig = TripServiceSignature {
                            trip_sig,
                            service_id: service_id.clone(),
                        };

                        // Only write this trip+service combination if we haven't seen it before
                        if !trip_service_to_id.contains_key(&trip_service_sig) {
                            // Generate trip_id based on UID
                            let base_uid = trip.uid.clone();
                            let usage_count = uid_usage_count.entry(base_uid.clone()).or_insert(0);
                            
                            let new_trip_id = if *usage_count == 0 {
                                // First usage - use UID directly
                                base_uid.clone()
                            } else {
                                // Subsequent usage - append date and STP indicator
                                format!("{}_{}_{}", base_uid, trip.date_start, trip.stp_ind)
                            };
                            *usage_count += 1;
                            
                            trip_service_to_id.insert(trip_service_sig, new_trip_id.clone());
                            
                            // Write the trip entry
                            trips_w.serialize(Trip {
                                route_id: route_id.clone(),
                                service_id: service_id.clone(),
                                trip_id: new_trip_id.clone(),
                                trip_headsign: trip.dest_name.clone(),
                                trip_short_name: trip.train_identity.clone(),
                            })?;
                            
                            // Write stop_times for this trip
                            for stop in &trip.stops {
                                let mut updated_stop = stop.clone();
                                updated_stop.trip_id = new_trip_id.clone();
                                st_w.serialize(&updated_stop)?;
                            }
                        }
                    }
                }
            }
            "AA" => {
                assoc_w.serialize(Association {
                    base_uid: line.get(3..9).unwrap_or("").to_string(),
                    assoc_uid: line.get(9..15).unwrap_or("").to_string(),
                    start_date: format!("20{}", line.get(15..21).unwrap_or("")),
                    end_date: format!("20{}", line.get(21..27).unwrap_or("")),
                    days_run: line.get(27..34).unwrap_or("").to_string(),
                    category: line.get(34..36).unwrap_or("").to_string(),
                    location: line.get(37..44).unwrap_or("").trim().to_string(),
                    assoc_type: line.get(47..48).unwrap_or("").to_string(),
                    stp_indicator: line.get(79..80).unwrap_or("").to_string(),
                })?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn format_time(raw: &str) -> String {
    let clean: String = raw.chars().filter(|c| c.is_numeric()).collect();
    if clean.len() >= 4 {
        format!("{}:{}:00", &clean[0..2], &clean[2..4])
    } else {
        "00:00:00".to_string()
    }
}

fn get_lo_line_details(
    stops: &[StopTime],
    tiploc_map: &HashMap<String, ParsedStation>,
) -> (String, String, String) {
    let mut names: HashSet<String> = HashSet::new();

    for stop in stops {
        if let Some(station) = tiploc_map.get(&stop.stop_id) {
            names.insert(station.name.clone());
        }
    }

    let has = |s: &str| -> bool {
        names
            .iter()
            .any(|n| n.to_uppercase().contains(&s.to_uppercase()))
    };

    if has("GOSPEL OAK") && has("BARKING") {
        return (
            "Suffragette Line".to_string(),
            "LO-SUFFRAGETTE".to_string(),
            "008163".to_string(),
        );
    }

    if has("ROMFORD") && has("UPMINSTER") {
        return (
            "Liberty Line".to_string(),
            "LO-LIBERTY".to_string(),
            "676767".to_string(),
        );
    }

    if has("LIVERPOOL STREET") && (has("CHESHUNT") || has("ENFIELD TOWN") || has("CHINGFORD")) {
        return (
            "Weaver Line".to_string(),
            "LO-WEAVER".to_string(),
            "a90068".to_string(),
        );
    }

    if has("EUSTON") && has("WATFORD JUNCTION") {
        return (
            "Lioness Line".to_string(),
            "LO-LIONESS".to_string(),
            "f1b41c".to_string(),
        );
    }

    if has("SHOREDITCH HIGH STREET") {
        return (
            "Windrush Line".to_string(),
            "LO-WINDRUSH".to_string(),
            "dc2517".to_string(),
        );
    }

    if has("STRATFORD")
        || (has("RICHMOND") && has("WILLESDEN JUNCTION"))
        || has("CAMDEN ROAD")
        || has("HACKNEY CENTRAL")
    {
        return (
            "Mildmay Line".to_string(),
            "LO-MILDMAY".to_string(),
            "437ec1".to_string(),
        );
    }

    (
        "London Overground".to_string(),
        "LO-GENERIC".to_string(),
        "E66A1F".to_string(),
    )
}
