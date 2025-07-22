import json
import os
import pandas as pd
import pprint
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --------- Config ---------
DATA_PATH = "/home/rs/Downloads/open-data-master_football/open-data-master/data"
RAW_PATH_SAVE = "/home/rs/Desktop/football_project/data/raw_json"
OUTPUT_CSV_PATH = "/home/rs/Desktop/football_project/data/barca_filtered_events_after.csv"
TEAM_NAME = "Barcelona"
NUM_THREADS = 5

# --------- Make sure raw JSON save directory exists ---------
os.makedirs(RAW_PATH_SAVE, exist_ok=True)

# Load competitions.json
with open(os.path.join(DATA_PATH, "competitions.json"), "r", encoding="utf-8") as f:
    competitions = json.load(f)

#print(competitions[0])

# Filter for La Liga seasons between 2014 and 2017
la_liga_ids = []
for comp in competitions:
    if comp["competition_name"] == "La Liga" and comp["season_name"] in ["2017/2018", "2018/2019", "2019/2020"]:
        la_liga_ids.append((comp["competition_id"], comp["season_id"]))

print("La Liga Seasons Found:", la_liga_ids)

#for comp in competitions:
#   print(f"{comp['competition_name']} | {comp['season_name']}")

barca_matches = []

# Loop through each season and load matches
for comp_id, season_id in la_liga_ids:
    match_path = os.path.join(DATA_PATH, "matches", str(comp_id), str(season_id) + ".json")
    
    # Open match file
    with open(match_path, "r", encoding="utf-8") as f:
        matches = json.load(f)
    
    for match in matches:
        home_team = match["home_team"]["home_team_name"]
        away_team = match["away_team"]["away_team_name"]

        if home_team == "Barcelona" or away_team == "Barcelona":
            barca_matches.append(match)

print(f"Total Barcelona matches found: {len(barca_matches)}")

def process_match(match):
    match_id = match["match_id"]
    EVENT_PATH = os.path.join(DATA_PATH, "events", f"{match_id}.json")

    try:
        with open(EVENT_PATH, "r", encoding="utf-8") as event_file:
            events = json.load(event_file)
    except FileNotFoundError:
        print(f"Warning: Event file not found for match {match_id}. Skipping.")
        return []

    with open(os.path.join(RAW_PATH_SAVE, f"{match_id}.json"), "w", encoding="utf-8") as event_writer:
        json.dump(events, event_writer, indent=2)

    filtered_rows = []
    
    for event in events:
        event_type = event["type"]["name"]
        team_name = event.get("team", {}).get("name")
        possession_team = event.get("possession_team", {}).get("name")
        
        # --- ROBUST LOCATION HANDLING ---
        # Get the location list first, providing a safe default
        location = event.get("location", [None, None])
        loc_x = location[0] if location else None
        loc_y = location[1] if len(location) > 1 else None

        # Case 1: Barça possession
        if team_name == "Barcelona" and possession_team == "Barcelona" and event_type in ["Pass", "Shot", "Dribble", "Ball Receipt*", "Carry"]:
            row = {
                "match_id": match_id, "event_type": event_type, "minute": event["minute"],
                "second": event["second"], "team": team_name, "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": loc_x, "location_y": loc_y,
            }
            if event_type == "Pass":
                pass_data = event.get("pass", {})
                end_loc = pass_data.get("end_location", [None, None])
                row["pass_end_x"] = end_loc[0] if end_loc else None
                row["pass_end_y"] = end_loc[1] if len(end_loc) > 1 else None
                # ... other pass data
                row["pass_outcome"] = pass_data.get("outcome", {}).get("name")
                row["recipient"] = pass_data.get("recipient", {}).get("name")
                row["pass_length"] = pass_data.get("length")
                row["pass_technique"] = pass_data.get("technique", {}).get("name")
                row["goal_assist"] = pass_data.get("goal_assist", False)
                row["shot_assist"] = pass_data.get("shot_assist", False)

            if event_type == "Shot":
                shot_data = event.get("shot", {})
                # --- FIX APPLIED HERE ---
                end_loc = shot_data.get("end_location", [None, None, None])
                row["shot_end_location_x"] = end_loc[0] if end_loc else None
                row["shot_end_location_y"] = end_loc[1] if len(end_loc) > 1 else None
                row["shot_end_location_z"] = end_loc[2] if len(end_loc) > 2 else None
                # ... other shot data
                row["shot_outcome"] = shot_data.get("outcome", {}).get("name")
                row["shot_statsbomb_xg"] = shot_data.get("statsbomb_xg")
                row["shot_technique"] = shot_data.get("technique", {}).get("name")
                row["shot_type"] = shot_data.get("type", {}).get("name")

            if event_type == "Dribble":
                dribble_data = event.get("dribble", {})
                row["dribble_nutmeg"] = dribble_data.get("nutmeg", False)
                row["dribble_overrun"] = dribble_data.get("overrun", False)
                row["dribble_outcome"] = dribble_data.get("outcome", {}).get("name")

            if event_type == "Carry":
                carry_data = event.get("carry", {})
                end_loc = carry_data.get("end_location", [None, None])
                row["carry_end_location_x"] = end_loc[0] if end_loc else None
                row["carry_end_location_y"] = end_loc[1] if len(end_loc) > 1 else None

            filtered_rows.append(row)
                
        # Case 2: Barça defending
        elif team_name == "Barcelona" and possession_team != "Barcelona" and event_type in ["Duel", "Block", "Interception", "Goal Keeper", "Pressure"]:
            row = {
                "match_id": match_id, "event_type": event_type, "minute": event["minute"],
                "second": event["second"], "team": team_name, "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": loc_x, "location_y": loc_y,
            }
            # ... (rest of the code for defensive actions is fine as it doesn't access end_locations)
            if event_type == "Duel":
                duel_data = event.get("duel", {})
                row["duel_outcome"] = duel_data.get("outcome", {}).get("name")
                row["duel_type"] = duel_data.get("type", {}).get("name")
            if event_type == "Block":
                row["block_deflection"] = event.get("block", {}).get("deflection", False)
            if event_type == "Interception":
                row["interception_outcome"] = event.get("interception", {}).get("outcome", {}).get("name")
            if event_type == "Goal Keeper":
                keeper_data = event.get("goalkeeper", {})
                row["gk_type"], row["gk_technique"], row["gk_outcome"], row["gk_position"], row["gk_body_part"] = keeper_data.get("type", {}).get("name"), keeper_data.get("technique", {}).get("name"), keeper_data.get("outcome", {}).get("name"), keeper_data.get("position", {}).get("name"), keeper_data.get("body_part", {}).get("name")
            if event_type == "Pressure":
                row["pressure_counterpress"] = event.get("pressure", {}).get("counterpress", False)
            
            filtered_rows.append(row)

        # Case 3: Opponent defending Barça
        elif team_name != "Barcelona" and possession_team == "Barcelona" and event_type in ["Duel", "Pressure", "Clearance", "Goal Keeper"]:
            row = {
                "match_id": match_id, "event_type": event_type, "minute": event["minute"],
                "second": event["second"], "team": team_name, "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": loc_x, "location_y": loc_y,
            }
            # ... (rest of the code for opponent defensive actions)
            if event_type == "Duel":
                duel_data = event.get("duel", {})
                row["duel_outcome"] = duel_data.get("outcome", {}).get("name")
                row["duel_type"] = duel_data.get("type", {}).get("name")
            if event_type == "Goal Keeper":
                keeper_data = event.get("goalkeeper", {})
                row["gk_type"], row["gk_technique"], row["gk_outcome"], row["gk_position"], row["gk_body_part"] = keeper_data.get("type", {}).get("name"), keeper_data.get("technique", {}).get("name"), keeper_data.get("outcome", {}).get("name"), keeper_data.get("position", {}).get("name"), keeper_data.get("body_part", {}).get("name")
            if event_type == "Pressure":
                row["pressure_counterpress"] = event.get("pressure", {}).get("counterpress", False)

            filtered_rows.append(row)
            
    return filtered_rows


all_filtered_rows = []

def thread_wrapper(match):
    try:
        return process_match(match)
    except Exception as e:
        print(f"Error in match {match['match_id']}: {e}")
        return []

with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    results = list(tqdm(executor.map(thread_wrapper, barca_matches), total=len(barca_matches), desc="Processing matches"))

# Flatten the results into a single list
for match_rows in results:
    all_filtered_rows.extend(match_rows)

df = pd.DataFrame(all_filtered_rows)
df.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"\nFiltered data saved to: {OUTPUT_CSV_PATH}")


