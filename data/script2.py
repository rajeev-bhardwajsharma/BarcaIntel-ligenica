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
    match_id=match["match_id"]
    EVENT_PATH=os.path.join(DATA_PATH,"events",f"{match_id}.json")

    with open(EVENT_PATH,"r",encoding="utf-8") as event_file:
        events=json.load(event_file)

    with open(os.path.join(RAW_PATH_SAVE,f"{match_id}.json"),"w",encoding="utf-8") as event_writer:
        json.dump(events,event_writer,indent=2)

    filtered_rows = []
    
    for event in events:
        event_type = event["type"]["name"]
        team_name = event.get("team", {}).get("name")
        possession_team = event.get("possession_team", {}).get("name")
    
        # Case 1: Barça possession
        if team_name == "Barcelona" and possession_team == "Barcelona" and event_type in ["Pass", "Shot", "Dribble", "Ball Receipt*", "Carry"]:
            row = {
                "match_id": match_id,
                "event_type": event_type,
                "minute": event["minute"],
                "second": event["second"],
                "team": team_name,
                "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": event.get("location", [None, None])[0],
                "location_y": event.get("location", [None, None])[1],
            }
            if event_type == "Pass":
                pass_data = event.get("pass", {})
                
                row["pass_outcome"] = pass_data.get("outcome", {}).get("name")
                row["pass_end_x"] = pass_data.get("end_location", [None, None])[0]
                row["pass_end_y"] = pass_data.get("end_location", [None, None])[1]
                row["recipient"] = pass_data.get("recipient", {}).get("name")
                row["pass_length"] = pass_data.get("length")
                row["pass_technique"] = pass_data.get("technique", {}).get("name")
                row["goal_assist"] = pass_data.get("goal_assist", False)
                row["shot_assist"] = pass_data.get("shot_assist", False)
            if event_type == "Shot":
                shot_data=event.get("shot",{})

                row["shot_end_location"]=shot_data.get("end_location",[None,None])[0]
                row["shot_outcome"]=shot_data.get("outcome",{}).get("name")
                row["shot_probability_of_goal"]=shot_data.get("statsbomb_xg")
                row["shot_technique"]=shot_data.get("technique",{}).get("name")
                row["shot_type"]=shot_data.get("type",{}).get("name")

           


            if event_type == "Dribble":
                dribble_data=event.get("dribble",{})

                row["dribble_skill_full_nutmeg"]=dribble_data.get("nutmeg",False)
                row["dribble_overrun"]=dribble_data.get("overrun",False)
                row["dribble_outcome"]=dribble_data.get("outcome",{}).get("name")


            if event_type == "Carry":
                carry_data=event.get("carry",{})

                row["carry_end_location"]=carry_data.get("end_location",[None,None])[0]

            filtered_rows.append(row)
                

    
        # Case 2: Barça defending
        elif team_name == "Barcelona" and possession_team != "Barcelona" and event_type in ["Duel", "Block", "Interception", "Goal Keeper", "Pressure"]:
            row = {
                "match_id": match_id,
                "event_type": event_type,
                "minute": event["minute"],
                "second": event["second"],
                "team": team_name,
                "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": event.get("location", [None, None])[0],
                "location_y": event.get("location", [None, None])[1],
            }
        
            # --- DUEL ---
            if event_type == "Duel":
                duel_data = event.get("duel", {})
                row["duel_outcome"] = duel_data.get("outcome", {}).get("name")
                row["duel_type"] = duel_data.get("type", {}).get("name")
        
            # --- BLOCK ---
            if event_type == "Block":
                block_data = event.get("block", {})
                row["block_deflection"] = block_data.get("deflection", False)
        
            # --- INTERCEPTION ---
            if event_type == "Interception":
                interception_data = event.get("interception", {})
                row["interception_outcome"] = interception_data.get("outcome", {}).get("name")
        
            # --- GOAL KEEPER ---
            if event_type == "Goal Keeper":
                keeper_data = event.get("goalkeeper", {})
                row["gk_type"] = keeper_data.get("type", {}).get("name")
                row["gk_technique"] = keeper_data.get("technique", {}).get("name")
                row["gk_outcome"] = keeper_data.get("outcome", {}).get("name")
                row["gk_position"] = keeper_data.get("position", {}).get("name")
                row["gk_body_part"] = keeper_data.get("body_part", {}).get("name")
        
            # --- PRESSURE ---
            if event_type == "Pressure":
                pressure_data = event.get("pressure", {})
                row["pressure_applied"] = pressure_data.get("counterpress", False)
        
            filtered_rows.append(row)


    
        # Case 3: Opponent defending Barça
        elif team_name != "Barcelona" and possession_team == "Barcelona" and event_type in ["Duel", "Pressure", "Clearance", "Goal Keeper"]:
            row = {
                "match_id": match_id,
                "event_type": event_type,
                "minute": event["minute"],
                "second": event["second"],
                "team": team_name,
                "possession_team": possession_team,
                "player": event.get("player", {}).get("name"),
                "play_pattern": event.get("play_pattern", {}).get("name", "Unknown"),
                "location_x": event.get("location", [None, None])[0],
                "location_y": event.get("location", [None, None])[1],
            }
            if event_type == "Duel":
                duel_data = event.get("duel", {})
                row["duel_outcome"] = duel_data.get("outcome", {}).get("name")
                row["duel_type"] = duel_data.get("type", {}).get("name")


            # --- GOAL KEEPER ---
            if event_type == "Goal Keeper":
                keeper_data = event.get("goalkeeper", {})
                row["gk_type"] = keeper_data.get("type", {}).get("name")
                row["gk_technique"] = keeper_data.get("technique", {}).get("name")
                row["gk_outcome"] = keeper_data.get("outcome", {}).get("name")
                row["gk_position"] = keeper_data.get("position", {}).get("name")
                row["gk_body_part"] = keeper_data.get("body_part", {}).get("name")
        
            # --- PRESSURE ---
            if event_type == "Pressure":
                pressure_data = event.get("pressure", {})
                row["pressure_applied"] = pressure_data.get("counterpress", False)

            

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


