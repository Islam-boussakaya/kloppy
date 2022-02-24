from kloppy.infra.serializers.event.InStat import (
    InstatDeserializer,
    InStatInputs,
)
from kloppy.domain import EventDataset, Optional, List
from kloppy.io import open_as_file, FileLike


def load(
    lineup_data: FileLike,
    events_data: FileLike,
    event_types: Optional[List[str]] = None,
    coordinates: Optional[str] = None,
) -> EventDataset:
    """
    Load Instat event data into a [`EventDataset`][kloppy.domain.models.event.EventDataset]
    Parameters:
        lineup_data: filename of xml containing the lineup information
        events_data: filename of xml containing the events 
        event_types:
        coordinates:
    """
    deserializer = InstatDeserializer(
        event_types=event_types, coordinate_system=coordinates
    )
    with open_as_file(lineup_data) as lineup_data_fp, open_as_file(
        events_data
    ) as events_data_fp:

        return deserializer.deserialize(
            inputs=InStatInputs(lineup_data = lineup_data_fp, events_data = events_data_fp),
        )