from typing import Tuple, Dict, List, NamedTuple, IO
import logging
from datetime import datetime
import pytz
from lxml import objectify
import re


from kloppy.domain import (
    EventDataset,
    Team,
    Period,
    Point,
    BallState,
    DatasetFlag,
    Orientation,
    PassEvent,
    ShotEvent,
    TakeOnEvent,
    GenericEvent,
    PassResult,
    ShotResult,
    TakeOnResult,
    Ground,
    Score,
    Provider,
    Metadata,
    Player,
    Position,
    RecoveryEvent,
    BallOutEvent,
    FoulCommittedEvent,
    FormationChangeEvent,
    FormationType,
    CardEvent,
    CardType,
    CardQualifier,
    SetPieceQualifier,
    SetPieceType,
    BodyPartQualifier,
    BodyPart,
    PassType,
    PassQualifier,
)

from kloppy.exceptions import DeserializationError
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.utils import performance_logging

logger = logging.getLogger(__name__)

EVENT_TYPE_CARD = ["3020","3030","3100"]
EVENT_QUALIFIER_FIRST_YELLOW_CARD = "3020"
EVENT_QUALIFIER_SECOND_YELLOW_CARD = "3100"
EVENT_QUALIFIER_RED_CARD = "3030"
EVENT_TYPE_PASS = []
EVENT_TYPE_FOUL_COMMITTED = "3010"
EVENT_TYPE_1ST_HALF = "18010"
EVENT_TYPE_2ND_HALF = "18020"
EVENT_TYPE_RECOVERY = "2060"
EVENT_TYPE_BALL_OUT = "27000"
EVENT_TYPE_CORNER_AWARDED ="5060"
BALL_OUT_EVENTS = [EVENT_TYPE_BALL_OUT, EVENT_TYPE_CORNER_AWARDED]

timestamp_match = 160000002

action_type_names = {
    1011: "Attacking pass accurate",
    1012: "Attacking pass inaccurate",
    1021: "Non attacking pass accurate",
    1022: "Non attacking pass inaccurate",
    1031: "Accurate key pass",
    1032: "Inaccurate key pass",
    1040: "Assist",
    1050: "Key assist",
    2010: "challenge",
    2020: "Air challenge",
    2030: "Tackle",
    8010: "Goal",
   26000: "Cross",
   26001: "Crosses accurate",
   26002: "Crosses inaccurate",

}

def _get_action_name(type_id: int) -> list:
    return action_type_names.get(type_id, "unknown")

def _parse_team(lineup_root, team_root , team_side
                    )-> Team:
    team_id = team_root.attrib["id"]
    formation = "-".join(re.findall(r'\d+', team_root.lineup.main.attrib["starting_tactic"]))
    team = Team(
        team_id=str(team_id),
        name=team_root.attrib["name"],
        ground=Ground.HOME
        if str(team_side) == "first_team"
        else Ground.AWAY,
        starting_formation=FormationType(formation),
    )
    team.players = [
        Player(
            player_id=player_elm.attrib["id"],
            team=team,
            jersey_no=int(player_elm.attrib["num"]),
            first_name=player_elm.attrib["firstname"],
            last_name=player_elm.attrib["lastname"],
            starting=True if player_elm.attrib["starting_lineup"] == 1 else False,
            position=Position(
                position_id=player_elm.attrib["starting_position_id"],
                name=player_elm.attrib["starting_position_name"],
                coordinates=None,
            ),
        )
        for player_elm in team_root.lineup.main.iterchildren("player")
    ]
    return team , team_id

def _parse_score (events_root,home_team_id,away_team_id):
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

def _parse_card(action_id: str) -> Dict:
    qualifiers = get_event_card_qualifier(action_id)
    if action_id == "3030":
        card_type = CardType.RED
    elif action_id == "3100":
        card_type = CardType.SECOND_YELLOW
    elif action_id == "3020":
        card_type = CardType.FIRST_YELLOW
    else:
        card_type = None

    return dict(result=None, qualifiers=qualifiers, card_type=card_type)

                                          
def get_event_card_qualifier(action_id: str):
    qualifiers = []
    if action_id == EVENT_QUALIFIER_FIRST_YELLOW_CARD:
         qualifiers.append(CardQualifier(value=CardType.FIRST_YELLOW))
    elif action_id == EVENT_QUALIFIER_SECOND_YELLOW_CARD:
        qualifiers.append(CardQualifier(value=CardType.SECOND_YELLOW))
    elif action_id == EVENT_QUALIFIER_RED_CARD:
        qualifiers.append(CardQualifier(value=CardType.RED))

    return qualifiers


class InStatInputs(NamedTuple):
    lineup_data: IO[bytes]
    events_data: IO[bytes]


class InstatDeserializer(EventDataDeserializer[InStatInputs]):
    @property
    def provider(self) -> Provider:
        return Provider.INSTAT
    
    def deserialize(self, inputs: InStatInputs) -> EventDataset:
        transformer = self.get_transformer(length=52.5, width=68)
        
        with performance_logging("load data", logger=logger):
            lineup_root = objectify.fromstring(inputs.lineup_data.read())
            events_root = objectify.fromstring(inputs.events_data.read())
            
        with performance_logging("parse data", logger=logger):
            home_team_root = lineup_root.first_team
            away_team_root = lineup_root.second_team
        
            home_team , home_team_id= _parse_team(lineup_root,home_team_root,"first_team")
            away_team , away_team_id = _parse_team(lineup_root,away_team_root,"second_team")
    
            home_score, away_score = _parse_score(events_root,home_team_id,away_team_id) 
            score = Score(home=home_score, away=away_score)
            teams = [home_team, away_team]
            row_elm = events_root.find("data")
    
    
            periods = [
                Period(
                    id=1,
                    start_timestamp=None,
                    end_timestamp=None,
                ),
                Period(
                    id=2,
                    start_timestamp=None,
                    end_timestamp=None,
                ),
            ]
            possession_team = None
            events = []
           
            for row_elm in events_root.iterchildren("row"):
                event_id = row_elm.attrib["id"]
                action_id = row_elm.attrib["action_id"]
                period_id = int(row_elm.attrib["half"])
                timestamp = timestamp_match + float(row_elm.attrib["second"])
                for period in periods:
                    if period.id == period_id and action_id == EVENT_TYPE_1ST_HALF:
                        period.start_timestamp = timestamp
                    
                    elif period.id == period_id and action_id == EVENT_TYPE_2ND_HALF:
                        period.start_timestamp = timestamp
                if period_id == 1:
                    period = periods[0]
                elif period_id == 2 :
                    period = periods[1]
                
                if 'possession_id' in row_elm.attrib:
                    if row_elm.attrib["team_id"] == home_team.team_id:
                        team = home_team
                    elif row_elm.attrib["team_id"] == away_team.team_id:
                        team = away_team
                    else:
                        raise DeserializationError(
                            f"Unknown team_id {row_elm.attrib['team_id']}"
                       )
            
                    x = float(row_elm.attrib["pos_x"])
                    y = float(row_elm.attrib["pos_y"])
            
                    player = None
                    if "player_id" in row_elm.attrib:
                        player = team.get_player_by_id(
                            row_elm.attrib["player_id"]
                        )
                    
                    possession_team = team
                    generic_event_kwargs = dict(
                        # from DataRecord
                        period=period,
                        timestamp=timestamp - period.start_timestamp,
                        ball_owning_team=possession_team,
                        ball_state=BallState.ALIVE,
                        # from Event
                        event_id=action_id,
                        team=team,
                        player=player,
                        coordinates=Point(x=x, y=y),
                        raw_event=row_elm,
                    )
                if action_id in EVENT_TYPE_CARD:
                    generic_event_kwargs["ball_state"] = BallState.DEAD
                    card_event_kwargs = _parse_card(action_id)
                    event = CardEvent.create(
                        **card_event_kwargs,
                        **generic_event_kwargs,)
                elif action_id == EVENT_TYPE_FOUL_COMMITTED:
                    event = FoulCommittedEvent.create(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                        )
            
                elif action_id == EVENT_TYPE_RECOVERY:
                    event = RecoveryEvent.create(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                        )
            
                elif action_id in BALL_OUT_EVENTS:
                    generic_event_kwargs["ball_state"] = BallState.DEAD
                    event = BallOutEvent.create(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                        )
        
                '''else:
                    event = GenericEvent.create(
                        **generic_event_kwargs,
                        result=None,
                        qualifiers=None,
                        event_name=_get_action_name(action_id),
                        )
                if self.should_include_event(event):
                    events.append(transformer.transform_event(event))'''

        metadata = Metadata(
            teams=teams,
            periods=periods,
            pitch_dimensions=transformer.get_to_coordinate_system().pitch_dimensions,
            score=score,
            frame_rate=None,
            orientation=Orientation.ACTION_EXECUTING_TEAM,
            flags=DatasetFlag.BALL_OWNING_TEAM,
            provider=Provider.OPTA,
            coordinate_system=transformer.get_to_coordinate_system(),
        )

        return EventDataset(
            metadata=metadata,
            records=events,
        )