from kloppy.io import open_as_file, FileLike
from typing import Tuple, Dict, List, NamedTuple, IO
import logging
from datetime import datetime
import pytz
import re
from lxml import objectify


from kloppy.domain import (
    EventDataset,
    Team,
    Point,
    Ground,
    Score,
    Player,
    FormationChangeEvent,
    FormationType,
   
)

from kloppy.exceptions import DeserializationError
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.utils import performance_logging

logger = logging.getLogger(__name__)


def _parse_events_datetime(dt_str: str) -> float:
    return (
        datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f")
        .replace(tzinfo=pytz.utc)
        .timestamp()
    )

  
def _parse_team_players(
    lineup_root, team_ref: str
) -> Tuple[Dict[str, Dict[str, str]]]:
    matchdata_path = objectify.ObjectPath("data."+team_ref+".lineup")
    team_elms = list(matchdata_path.find(lineup_root).iterchildren("main"))
    for team_elm in team_elms:
        players = {
            player_elm.attrib["id"]: dict(
                first_name=str(
                    player_elm.attrib["firstname"]
                    ),
                last_name=str(player_elm.attrib["lastname"]),
                )
            for player_elm in team_elm.iterchildren("player")
        }
        break
    else:
        raise DeserializationError(f"Could not parse players for {team_ref}")

    return players

  
def _parse_team(lineup_root, team_root , team_side
                    )-> Team:
    team_players = _parse_team_players(lineup_root,str(team_side))
    team_id = team_root.attrib["id"]
    team_name = team_root.attrib["name"]
    formation = "-".join(re.findall(r'\d+', team_root.lineup.main.attrib["starting_tactic"]))
    team = Team(
        team_id = str(team_id),
        name = str(team_name),
        ground = Ground.HOME
        if str(team_side) == "first_team"
        else Ground.AWAY, 
        starting_formation=FormationType(formation),
    )
    
    for player_elm in team_root.lineup.main.iterchildren("player"):
        team.players = [
            Player(
            player_id = player_elm.attrib["id"],
            first_name = player_elm.attrib["firstname"],
            last_name = str(player_elm.attrib["lastname"]),
            team = team,
            jersey_no = int(player_elm.attrib["num"]),
            starting = True if player_elm.attrib["starting_lineup"] == 1 else False,
            position = Position(position_id=player_elm.attrib["starting_position_id"],
                        name=player_elm.attrib["starting_position_name"],
                        coordinates=None,),
                
                )     
        ]
    return team , team_id
 

def _parse_score (events_root,home_team_id,away_home_id):
    home_score = 0
    away_score = 0
    try:
        for event in events_root.iterchildren("row"):
            if event.attrib["action_id"]=="8010" and event.attrib["team_id"]==str(home_team_id):
                home_score +=1
            elif event.attrib["action_id"]=="8010" and event.attrib["team_id"]==str(away_team_id):
                away_score +=1
    
    except KeyError:
        pass
    return home_score,away_score
 

class InStatInputs(NamedTuple):
    lineup_data: IO[bytes]
    events_data: IO[bytes]

