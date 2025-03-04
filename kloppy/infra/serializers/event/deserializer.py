from abc import ABC, abstractmethod
from typing import Optional, List, Generic, TypeVar, Union

from kloppy.domain import (
    EventDataset,
    Event,
    EventType,
    Transformer,
    Provider,
    build_coordinate_system,
)

T = TypeVar("T")


class EventDataDeserializer(ABC, Generic[T]):
    def __init__(
        self,
        event_types: Optional[List[Union[EventType, str]]] = None,
        coordinate_system: Optional[Union[str, Provider]] = None,
    ):
        if not event_types:
            event_types = []

        self.event_types = [
            EventType[event_type.upper()]
            if isinstance(event_type, str)
            else event_type
            for event_type in event_types
        ]

        if not coordinate_system:
            coordinate_system = Provider.KLOPPY

        if isinstance(coordinate_system, str):
            coordinate_system = Provider[coordinate_system.upper()]

        self.coordinate_system = coordinate_system

    def should_include_event(self, event: Event) -> bool:
        if not self.event_types:
            return True
        return event.event_type in self.event_types

    def get_transformer(self, length: float, width: float) -> Transformer:
        from_coordinate_system = build_coordinate_system(
            self.provider,
            length=length,
            width=width,
        )

        to_coordinate_system = build_coordinate_system(
            self.coordinate_system,
            length=length,
            width=width,
        )

        return Transformer(
            from_coordinate_system=from_coordinate_system,
            to_coordinate_system=to_coordinate_system,
        )

    @property
    @abstractmethod
    def provider(self) -> Provider:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, inputs: T) -> EventDataset:
        raise NotImplementedError
