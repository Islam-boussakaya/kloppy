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
from . import instat_events
logger = logging.getLogger(__name__)

class InStatInputs(NamedTuple):
    lineup_data: IO[bytes]
    events_data: IO[bytes]

action_type_names = {
    1011: "Attacking pass accurate",
    1012: "Attacking pass inaccurate",
    1021: "Non attacking pass accurate",
    1022: "Non attacking pass inaccurate",
    1031: "Accurate key pass",
    1032: "Inaccurate key pass",
    2010: "challenge",
    2020: "Air challenge",
    2030: "Tackle"

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

def _parse_card(
    action_id: str
) -> Dict:

    qualifiers = []
    if action_id == instat_events.EVENT_TYPE_RED_CARD:
        card_type = CardType.RED
        qualifiers = qualifiers.append(CardQualifier(value=CardType.RED))
    elif action_id == instat_events.EVENT_TYPE_SECOND_YELLOW_CARD:
        card_type = CardType.SECOND_YELLOW
        qualifiers = qualifiers.append(CardQualifier(value=CardType.SECOND_YELLOW))
    elif action_id == instat_events.EVENT_TYPE_FIRST_YELLOW_CARD:
        card_type = CardType.FIRST_YELLOW
        qualifiers = qualifiers.append(CardQualifier(value=CardType.FIRST_YELLOW))
    else:
        card_type = None

    return dict(result=None, qualifiers=qualifiers, card_type=card_type)

def _parse_pass(
    action_id: str, row_elm
) -> Dict:
    
    qualifiers = []
    if action_id in instat_events.EVENT_TYPE_CROSS:
        if action_id in instat_events.EVENT_TYPE_CROSS_INCOMPLETE:
            result = PassResult.INCOMPLETE
        elif action_id in instat_events.EVENT_TYPE_CROSS_COMPLETE:
            result = PassResult.COMPLETE
        qualifiers = qualifiers.append(PassQualifier(value=PassType.CROSS))
    
    elif action_id in instat_events.EVENT_TYPE_ASSIST:
        result = PassResult.COMPLETE
        qualifiers = qualifiers.append(PassQualifier(value=PassType.ASSIST))
    
    elif action_id in instat_events.EVENT_TYPE_ASSISIT_2ND:
        result = PassResult.COMPLETE
        qualifiers = qualifiers.append(PassQualifier(value=PassType.ASSIST_2ND))
    
    receiver_coordinates = Point(
            x=float(row_elm.attrib["pos_dest_x"]), y=float(row_elm.attrib["pos_dest_y"])
        )

    return dict(
        result=result,
        receiver_coordinates=receiver_coordinates,
        receiver_player=None,
        receive_timestamp=None,
        qualifiers=qualifiers,
    )


def _parse_shot(
    action_id: str, coordinates: Point
) -> Dict:
    if action_id == instat_events.EVENT_TYPE_SHOT_GOAL:
        result = ShotResult.GOAL
        
    elif action_id == instat_events.EVENT_TYPE_SHOT_OWN_GOAL:
        result = ShotResult.OWN_GOAL
    
    elif action_id == instat_events.EVENT_TYPE_SHOT_BLOCKED:
        result = ShotResult.BLOCKED
    
    elif action_id == instat_events.EVENT_TYPE_SHOT_POST:
        result = ShotResult.POST
    
    elif action_id == instat_events.EVENT_TYPE_SHOT_SAVED:
        result = ShotResult.SAVED
        
    else:
        result = None
    qualifiers = []
    return dict(coordinates=coordinates, result=result, qualifiers=qualifiers)

def _parse_take_on(action_id: str) -> Dict:
    if action_id in instat_events.EVENT_TYPE_TAKE_ON_COMPLETE:
        result = TakeOnResult.COMPLETE
    elif action_id == instat_events.EVENT_TYPE_TAKE_ON_INSUCC_DRIBBLING:
        result = TakeOnResult.INCOMPLETE
    return dict(result=result)

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
                timestamp = instat_events.timestamp_match + float(row_elm.attrib["second"])
                for period in periods:
                    if period.id == period_id and action_id == instat_events.EVENT_TYPE_1ST_HALF:
                        period.start_timestamp = timestamp
                    
                    elif period.id == period_id and action_id == instat_events.EVENT_TYPE_2ND_HALF:
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
                        raise DeserializationError(f"Unknown team_id {row_elm.attrib['team_id']}")
            
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
                    event_id=event_id,
                    team=team,
                    player=player,
                    coordinates=Point(x=x, y=y),
                    raw_event=row_elm,
                    )
            
                    if action_id == instat_events.EVENT_TYPE_FOUL_COMMITTED:
                        event = FoulCommittedEvent.create(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,)
                        events.append(transformer.transform_event(event))
                
                    elif action_id in instat_events.BALL_OUT_EVENTS:
                        generic_event_kwargs["ball_state"] = BallState.DEAD
                        event = BallOutEvent.create(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                        )
                        events.append(transformer.transform_event(event))
                    
                    elif action_id in instat_events.EVENT_TYPE_PASS:
                        pass_event_kwargs = _parse_pass(
                        action_id, row_elm
                        )
                        event = PassEvent.create(
                        **pass_event_kwargs,
                        **generic_event_kwargs,
                        )
                        events.append(transformer.transform_event(event))
        
            
                    elif action_id in instat_events.EVENT_TYPE_SHOT:
                        shot_event_kwargs = _parse_shot(
                            action_id,
                            coordinates=generic_event_kwargs["coordinates"],
                            )
                        kwargs = {}
                        kwargs.update(generic_event_kwargs)
                        kwargs.update(shot_event_kwargs)
                        event = ShotEvent.create(**kwargs)
                        events.append(transformer.transform_event(event))
                    
                    elif action_id in instat_events.EVENT_TYPE_TAKE_ON:
                        take_on_event_kwargs = _parse_take_on(action_id)
                        event = TakeOnEvent.create(
                        qualifiers=None,
                        **take_on_event_kwargs,
                        **generic_event_kwargs,
                        )

                    else:
                        event = GenericEvent.create(
                        **generic_event_kwargs,
                        result=None,
                        qualifiers=None,
                        event_name=_get_action_name(action_id),
                        )
                        events.append(transformer.transform_event(event))


                if action_id in instat_events.EVENT_TYPE_CARD:
                    generic_event_kwargs["ball_state"] = BallState.DEAD
                    card_event_kwargs = _parse_card(action_id)
                    event = CardEvent.create(
                        **card_event_kwargs,
                        **generic_event_kwargs,)
                    events.append(transformer.transform_event(event))
               
        
                elif action_id == instat_events.EVENT_TYPE_RECOVERY:
                    event = RecoveryEvent.create(
                    result=None,
                    qualifiers=None,
                    **generic_event_kwargs,
                        )
                    events.append(transformer.transform_event(event))
  
                
        metadata = Metadata(
            teams=teams,
            periods=periods,
            pitch_dimensions=transformer.get_to_coordinate_system().pitch_dimensions,
            score=score,
            frame_rate=None,
            orientation=Orientation.ACTION_EXECUTING_TEAM,
            flags=DatasetFlag.BALL_OWNING_TEAM,
            provider=Provider.INSTAT,
            coordinate_system=transformer.get_to_coordinate_system(),
        )

        return EventDataset(
            metadata=metadata,
            records=events,
        )  