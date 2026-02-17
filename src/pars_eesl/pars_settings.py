import os
from pathlib import Path

BASE_URL = "https://fafr.su/"
BASE_ALL_PLAYERS_URL = BASE_URL + "participants/players"
BASE_PLAYER = BASE_URL + "player/"
BASE_SEASON_URL = BASE_URL + "tournaments?season_id="
BASE_TOURNAMENT_URL = BASE_URL + "tournament/"
BASE_TEAM_URL = BASE_URL + "team/"  # default
BASE_TEAM_TOURNAMENT_URL = BASE_TOURNAMENT_URL + "teams/application?team_id="  # tournament
BASE_MATCH_URL = BASE_URL + "match/"
SEASON_ID = int(os.getenv("CURRENT_SEASON_ID", "1"))
match_id = "29"
TEST_DOWNLOADS_PATH = str(Path(__file__).parent.parent.parent / "test_downloads")
