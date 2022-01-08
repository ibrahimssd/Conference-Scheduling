from dataclasses import dataclass, field
from typing import Mapping, NewType, Union


StreamID = NewType('StreamID', int)
RoomID = NewType('RoomID', int)
TimeblockID = NewType('TimeblockID', int)
AbstractID = NewType('AbstractID', int)

RoomCosts = Mapping[RoomID, float]
TimeblockCosts = Mapping[TimeblockID, float]
StreamCosts = Mapping[StreamID, float]


@dataclass(frozen=True)
class Stream:
    id: StreamID  # pylint: disable=invalid-name
    name: str
    room_costs: RoomCosts = field(compare=False,
                                  hash=False,
                                  repr=False)
    timeblock_costs: TimeblockCosts = field(compare=False,
                                            hash=False,
                                            repr=False)
    conflict_costs: StreamCosts = field(compare=False,
                                        hash=False,
                                        repr=False)
    max_days: Union[int, None] = None
    days_penalty: float = 0

    def room_cost(self, room: Union['Room', RoomID]) -> float:
        id_: RoomID = -1
        if isinstance(room, Room):
            id_ = room.id
        else:
            id_ = room
        return self.room_costs[id_]

    def timeblock_cost(self, timeblock: Union['Timeblock', TimeblockID]):
        id_: TimeblockID = -1
        if isinstance(timeblock, Timeblock):
            id_ = timeblock.id
        else:
            id_ = timeblock
        return self.timeblock_costs[id_]

    def conflict_cost(self, stream: StreamID):
        id_: StreamID = -1
        if isinstance(stream, Stream):
            id_ = stream.id
        else:
            id_ = stream
        return self.conflict_costs[id_]

    def has_max_days(self):
        return self.max_days is not None


@dataclass(frozen=True)
class Room:
    id: RoomID  # pylint: disable=invalid-name
    name: str
    stream_costs: StreamCosts = field(compare=False,
                                      hash=False,
                                      repr=False)
    timeblock_costs: TimeblockCosts = field(compare=False,
                                            hash=False,
                                            repr=False)

    def stream_cost(self, stream: StreamID):
        id_: StreamID = -1
        if isinstance(stream, Stream):
            id_ = stream.id
        else:
            id_ = stream
        return self.stream_costs[id_]

    def timeblock_cost(self, timeblock: Union['Timeblock', TimeblockID]):
        id_: TimeblockID = -1
        if isinstance(timeblock, Timeblock):
            id_ = timeblock.id
        else:
            id_ = timeblock
        return self.timeblock_costs[id_]


@dataclass(frozen=True)
class Timeblock:
    id: TimeblockID  # pylint: disable=invalid-name
    name: str
    day: int
    start: int
    num_timeslots: int
    stream_costs: StreamCosts = field(compare=False,
                                      hash=False,
                                      repr=False)
    room_costs: RoomCosts = field(compare=False,
                                  hash=False,
                                  repr=False)

    room_costs: RoomCosts = field(compare=False,
                                  hash=False,
                                  repr=False)

    def room_cost(self, room: Union['Room', RoomID]) -> float:
        id_: RoomID = -1
        if isinstance(room, Room):
            id_ = room.id
        else:
            id_ = room
        return self.room_costs[id_]

    def stream_cost(self, stream: Union['Stream', StreamID]) -> float:
        id_: StreamID = -1
        if isinstance(stream, Stream):
            id_ = stream.id
        else:
            id_ = stream
        return self.stream_costs[id_]


@dataclass(frozen=True)
class Abstract:
    id: AbstractID  # pylint: disable=invalid-name
    reference: str
    stream: StreamID
    timeslots: int
    timeblock_costs: TimeblockCosts = field(compare=False,
                                            hash=False,
                                            repr=False)
    order: Union[int, None] = None
    clash: Union[AbstractID, None] = None
    speaker_clash: Union[AbstractID, None] = None
