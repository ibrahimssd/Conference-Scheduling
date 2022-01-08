from collections import defaultdict
from typing import (
    Union,
    Mapping,
    Sequence, Iterable,
    Set, List,
)
import pandas as pd
from .types import (
    Stream, StreamID,
    Room, RoomID,
    Timeblock, TimeblockID,
    Abstract, AbstractID,
)

__all__ = ['Instance']


class Instance:
    def __init__(self,
                 streams: Iterable[Stream],
                 rooms: Iterable[Room],
                 timeblocks: Sequence[Timeblock],
                 abstracts: Iterable[Abstract]):
        self._streams = {
            stream.id: stream
            for stream in streams
        }
        self._rooms = {
            room.id: room
            for room in rooms
        }
        # dict's remember key orders since Python 3.7
        self._timeblocks = {
            timeblock.id: timeblock
            for timeblock in timeblocks
        }
        self._abstracts = {
            abstract.id: abstract
            for abstract in abstracts
        }

        self._streams_by_name = {
            self._streams[id_].name: self._streams[id_]
            for id_ in self._streams
        }

        self._abstracts_by_ref = {
            self._abstracts[id_].reference: self._abstracts[id_]
            for id_ in self._abstracts
        }

    # Streams
    @property
    def streams(self) -> Set[Stream]:
        return set(self._streams.values())

    def stream(self, id_: StreamID) -> Stream:
        return self._streams[id_]

    def stream_by_name(self, name: str) -> Stream:
        return self._streams_by_name[name]

    # Rooms
    @property
    def rooms(self) -> Set[Room]:
        return set(self._rooms.values())

    def room(self, id_: RoomID) -> Room:
        return self._rooms[id_]

    # Timeblocks
    @property
    def timeblocks(self) -> List[Timeblock]:
        return list(self._timeblocks.values())

    def timeblock(self, id_: TimeblockID) -> Timeblock:
        return self._timeblocks[id_]

    def timeblock_by_timeslot(self, timeslot: int) -> Timeblock:
        for timeblock in self.timeblocks:
            if timeblock.start >= timeslot:
                return timeblock
        raise ValueError(f'Time slot {timeslot} is out of bounds.')

    # Timeslots
    @property
    def num_timeslots(self) -> int:
        return sum(timeblock.num_timeslots for timeblock in self.timeblocks)

    # Abstracts
    @property
    def abstracts(self) -> Set[Abstract]:
        return set(self._abstracts.values())

    def abstract(self, id_: AbstractID) -> Abstract:
        return self._abstracts[id_]

    def abstract_by_ref(self, reference: str) -> Abstract:
        return self._abstracts_by_ref[reference]

    def abstracts_by_stream(self,
                            stream: Union[Stream, StreamID]) -> Set[Stream]:
        id_: StreamID
        if isinstance(stream, Stream):
            id_ = stream.id
        else:
            id_ = stream
        return {
            abstract
            for abstract in self.abstracts
            if abstract.stream.id == id_
        }

    @staticmethod
    def from_excel_data(data: Mapping[str, pd.DataFrame]) -> 'Instance':
        """ create an Instance object from data stored in excel spreadsheets
        """
        abstracts_df = data['abstracts']
        streams_df = data['streams']
        rooms_df = data['rooms']
        sessions_df = data['sessions']
        streams_rooms_df = data['streams_rooms|penalty']
        streams_timeblocks_df = data['streams_sessions|penalty']
        timeblocks_rooms_df = data['sessions_rooms|penalty']
        streams_streams_df = data['streams_streams|penalty']

        abstracts_keys = dict(zip(abstracts_df.Reference, abstracts_df.index))
        stream_keys = dict(zip(streams_df.Streams, streams_df.index))
        room_keys = dict(zip(rooms_df.Rooms, rooms_df.index))
        timeblock_keys = dict(zip(sessions_df.Sessions, sessions_df.index))

        streams = [Instance._stream_from_excel(id_,
                                               streams_df.loc[id_, :],
                                               streams_timeblocks_df,
                                               streams_rooms_df,
                                               streams_streams_df,
                                               timeblock_keys,
                                               room_keys,
                                               stream_keys
                                               )
                   for id_ in streams_df.index]

        abstracts = [Instance._abstract_from_excel(id_,
                                                   abstracts_df.loc[id_, :],
                                                   abstracts_keys,
                                                   timeblock_keys,
                                                   stream_keys
                                                   )
                     for id_ in abstracts_df.index]

        rooms = [Instance._room_from_excel(id_,
                                           rooms_df.loc[id_, :],
                                           streams_rooms_df,
                                           timeblocks_rooms_df,
                                           stream_keys,
                                           timeblock_keys
                                           )
                 for id_ in rooms_df.index]

        timeblocks = []
        start_timeslot = 0
        for id_ in sessions_df.index:
            timeblock = Instance._timeblock_from_excel(
                id_,
                sessions_df.loc[id_, :],
                start_timeslot,
                streams_timeblocks_df,
                timeblocks_rooms_df,
                stream_keys,
                room_keys
            )
            start_timeslot += timeblock.num_timeslots
            timeblocks.append(timeblock)

        return Instance(streams, rooms, timeblocks, abstracts)

    @staticmethod
    def _stream_from_excel(id_: StreamID,
                           stream_record: pd.Series,
                           streams_timeblocks: pd.DataFrame,
                           streams_rooms: pd.DataFrame,
                           streams_streams: pd.DataFrame,
                           timeblock_keys: Mapping[str, TimeblockID],
                           room_keys: Mapping[str, RoomID],
                           stream_keys: Mapping[str, StreamID]) -> Stream:
        """Creates a Stream object from an excel record"""
        name = stream_record.Streams
        room_costs = cost_dict(streams_rooms, name,
                               room_keys)
        timeblock_costs = cost_dict(streams_timeblocks, name,
                                    timeblock_keys)
        conflict_costs = cost_dict(streams_streams, name,
                                   stream_keys, axis=None)
        max_days = stream_record.at['Max Number of Days']
        if not pd.isna(max_days):
            max_days = int(max_days)
        else:
            max_days = None

        days_penalty = stream_record.at['Cost for Extra Days']
        if not pd.isna(days_penalty):
            days_penalty = float(days_penalty)
        else:
            days_penalty = 0

        return Stream(id_, name,
                      room_costs,
                      timeblock_costs,
                      conflict_costs,
                      max_days=max_days,
                      days_penalty=days_penalty)

    @staticmethod
    def _room_from_excel(id_: RoomID,
                         record: pd.Series,
                         streams_rooms: pd.DataFrame,
                         timeblocks_rooms: pd.DataFrame,
                         stream_keys: Mapping[str, StreamID],
                         timeblock_keys: Mapping[str, TimeblockID]) -> Room:
        """Creates a Room object from an excel record"""
        name = record.Rooms
        timeblock_costs = cost_dict(timeblocks_rooms, name,
                                    timeblock_keys, axis=1)
        stream_costs = cost_dict(streams_rooms, name,
                                 stream_keys, axis=1)
        return Room(id_, name, stream_costs, timeblock_costs)

    @staticmethod
    def _timeblock_from_excel(id_: TimeblockID,
                              record: pd.Series,
                              start_timeslot: int,
                              streams_timeblocks: pd.DataFrame,
                              timeblocks_rooms: pd.DataFrame,
                              stream_keys: Mapping[str, StreamID],
                              room_keys: Mapping[str, RoomID]) -> Timeblock:
        """Creates a Timeblock object from an excel record"""
        name = record.Sessions
        num_timeslots = record.at['Max Number of Talks']
        day = record.Day
        stream_costs = cost_dict(streams_timeblocks, name,
                                 stream_keys, axis=1)
        room_costs = cost_dict(timeblocks_rooms, name,
                               room_keys)
        return Timeblock(id_, name, day,
                         start_timeslot, num_timeslots,
                         stream_costs, room_costs)

    @staticmethod
    def _abstract_from_excel(id_: TimeblockID,
                             record: pd.Series,
                             abstract_keys: Mapping[str, AbstractID],
                             timeblock_keys: Mapping[str, TimeblockID],
                             stream_keys: Mapping[str, StreamID]) -> Abstract:
        """Creates an Abstract object from an excel record"""
        reference = record.Reference
        stream = stream_keys[record.Stream]
        timeslots = int(record.at['Required Timeslots'])
        order = record.Order
        if not pd.isna(order) and int(order) > 0:
            order = int(order)
        else:
            order = None
        timeblock_costs = {
            timeblock_keys[timeblock]: float(record.at[timeblock])
            for timeblock in timeblock_keys.keys()
            if not pd.isna(record.at[timeblock])
        }
        timeblock_costs = defaultdict(lambda: 0, timeblock_costs)
        clash_ref = record.at['Clash (Including same session/stream)']
        clash = None
        if not pd.isna(clash_ref):
            clash = abstract_keys[clash_ref]
        speaker_clash_ref = record.at['Clash (Speaker)']
        speaker_clash = None
        if not pd.isna(speaker_clash_ref):
            speaker_clash = abstract_keys[speaker_clash_ref]
        return Abstract(id_, reference, stream,
                        timeslots, timeblock_costs,
                        order=order,
                        clash=clash,
                        speaker_clash=speaker_clash)


def cost_dict(penalty_df: pd.DataFrame,
              name: str,
              keys: Mapping[str, int],
              axis: Union[int, None] = 0) -> Mapping[int, float]:
    """Constructs a cost dict from a penalty DataFrame"""
    costs: pd.Series = None
    if axis == 0:
        selector = penalty_df.iloc[:, 0] == name
        matching_costs = penalty_df.loc[selector, :]
        # take first matching row, drop the stream names columns
        costs = matching_costs.iloc[0, 1:]
    elif axis == 1:
        names = penalty_df.iloc[:, 0]
        selector = penalty_df.columns == name
        matching_costs = penalty_df.loc[:, selector]
        # take first matching column, and index with names
        costs = matching_costs.iloc[:, 0].set_axis(names)
    elif axis is None:
        names = penalty_df.iloc[:, 0]
        row = penalty_df.loc[names == name, :].iloc[0, 1:]
        col_selector = penalty_df.columns == name
        column = penalty_df.loc[:, col_selector].iloc[:, 0].set_axis(names)
        costs = row.mask(row.isna(), column)

    costs = {
        keys[room]: float(costs[room])
        for room in costs.index
        if not pd.isna(costs[room])
    }
    return defaultdict(lambda: 0, costs)
