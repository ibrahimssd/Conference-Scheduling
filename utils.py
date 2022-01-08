import sys
from itertools import dropwhile, takewhile
import numpy as np
import pandas as pd
from .config import (
    SOLUTION_STREAMS_SHEET,
    SOLUTION_ABSTRACTS_SHEET,
    SOLUTION_STREAMS_VIOLATIONS_SHEET,
    SOLUTION_ABSTRACTS_VIOLATIONS_SHEET,
)


def write_schedule(path,
                   streams_scheduler, abstracts_scheduler,
                   abstracts_df, streams_df, sessions_df, rooms_df):
    streams_solution = streams_scheduler.solution
    abstracts_solution = abstracts_scheduler.solution
    streams = streams_df['Streams']
    abstracts = abstracts_df['Reference']
    rooms = rooms_df['Rooms']

    sessions = sessions_df['Sessions']
    streams_schedule = pd.DataFrame(
        streams.to_numpy()[streams_solution].reshape(streams_solution.shape),
        index=sessions,
        columns=rooms
    ).mask(streams_solution == -1)

    timeslots = [sessions_df.at[session, 'Sessions']
                 for session in timeslot_to_session_map(sessions_df)]
    abstracts_schedule = pd.DataFrame(
        abstracts.to_numpy()[abstracts_solution].reshape(
            abstracts_solution.shape),
        index=timeslots,
        columns=rooms
    ).mask(abstracts_solution == -1)

    streams_violations = format_streams_violations(
        streams_scheduler.violations,
        streams,
        sessions,
        rooms)
    abstracts_violations = format_abstracts_violations(
        abstracts_scheduler.violations,
        abstracts,
        sessions)

    with pd.ExcelWriter(path, mode='w') as writer:  # pylint: disable=abstract-class-instantiated
        streams_schedule.to_excel(
            writer,
            SOLUTION_STREAMS_SHEET)
        abstracts_schedule.to_excel(
            writer,
            SOLUTION_ABSTRACTS_SHEET)
        streams_violations.to_excel(
            writer,
            SOLUTION_STREAMS_VIOLATIONS_SHEET)
        abstracts_violations.to_excel(
            writer,
            SOLUTION_ABSTRACTS_VIOLATIONS_SHEET)


def format_streams_violations(penalties, streams, sessions, rooms):
    violations = pd.concat((
        pd.Series(name='Stream VS Session', data=[
            f'"{streams.at[stream]}" VS "{sessions.at[session]}" ({penalty})'
            for stream, session, penalty in penalties.streams_sessions
        ]),
        pd.Series(name='Stream VS Room', data=[
            f'"{streams.at[stream]}" VS "{rooms.at[room]}" ({penalty})'
            for stream, room, penalty in penalties.streams_rooms
        ]),
        pd.Series(name='Session VS Room', data=[
            f'"{sessions.at[session]}" VS "{rooms.at[room]}" ({penalty})'
            for session, room, penalty in penalties.sessions_rooms
        ]),
        pd.Series(name='Stream VS Stream', data=[
            f'"{streams.at[stream]}" VS "{streams.at[other]}"'
            f' in "{sessions.at[session]}" ({penalty})'
            for stream, other, session, penalty in penalties.streams_streams
        ]),
        pd.Series(name='Parallel', data=[
            f'{streams.at[stream]} ({penalty})'
            for stream, penalty in penalties.parallel
        ]),
        pd.Series(name='Unscheduled', data=[
            f'{streams.at[stream]}'
            for stream in penalties.scheduled
        ]),
        pd.Series(name='Number of Rooms', data=[
            f'{streams.at[stream]} ({penalty})'
            for stream, penalty in penalties.number_of_rooms
        ]),
        pd.Series(name='Non-Consecutive Sessions', data=[
            f'"{streams.at[stream]}" in "{rooms.at[room]}" ({penalty})'
            for stream, room, penalty in penalties.consecutive
        ]),
    ), axis=1)

    counts = (
        map(lambda p: p[2], penalties.streams_sessions),
        map(lambda p: p[2], penalties.streams_rooms),
        map(lambda p: p[2], penalties.sessions_rooms),
        map(lambda p: p[3], penalties.streams_streams),
        map(lambda p: p[1], penalties.parallel),
        map(lambda _: 1, penalties.scheduled),
        map(lambda p: p[1], penalties.number_of_rooms),
        map(lambda p: p[2], penalties.consecutive),
    )

    totals = pd.Series([f'Total = {sum(c)}' for c in counts],
                       index=violations.columns)

    return violations.append(totals, ignore_index=True)


def format_abstracts_violations(penalties, abstracts, sessions):
    violations = pd.concat((
        pd.Series(name='Unscheduled', data=[
            f'{abstracts.at[abstract]}'
            for abstract in penalties.scheduled
        ]),
        pd.Series(name='Order', data=[
            f'{abstracts.at[abstract]} ({penalty})'
            for abstract, penalty in penalties.order
        ]),
        pd.Series(name='Abstract VS Session', data=[
            f'{abstracts.at[abstract]} VS {sessions.at[session]}'
            f' ({penalty})'
            for abstract, session, penalty in penalties.sessions
        ]),
        pd.Series(name='Abstract VS Abstract', data=[
            f'"{abstracts.at[abstract]}" VS "{abstracts.at[clash]}"'
            f' in "{sessions.at[session]}"'
            for abstract, clash, session in penalties.conflicts
        ]),
    ), axis=1)

    counts = (
        map(lambda _: 1, penalties.scheduled),
        map(lambda p: p[1], penalties.order),
        map(lambda p: p[2], penalties.sessions),
        map(lambda p: 1, penalties.conflicts),
    )
    totals = pd.Series([f'Total = {sum(c)}' for c in counts],
                       index=violations.columns)
    return violations.append(totals, ignore_index=True)


def get_stream_timeslots(stream, all_abstracts):
    timeslots = all_abstracts.loc[all_abstracts['Stream'] == stream,
                                  'Required Timeslots']
    return timeslots.sum()


def avg_talks_per_session(sessions):
    talks = sessions.loc[:, 'Max number of talks']
    return talks.mean() if len(talks) > 0 else 0


def required_sessions_per_stream(streams, abstracts, sessions):
    avg_timeslots = avg_talks_per_session(sessions)
    required_sessions = np.zeros(len(streams), dtype=np.int)
    for i in streams.index:
        stream = streams.loc[i, 'Streams']
        required_sessions[i] = get_stream_timeslots(stream, abstracts)
    return np.ceil(required_sessions / avg_timeslots)


def timeslot_to_session_map(sessions_df):
    return [session
            for session_index, numTalks in zip(sessions_df.index,
                                               sessions_df['Max number of talks'].tolist())
            for session in [session_index] * numTalks]


def unique_scheduled_elements(index, *arrays):
    elements = set()
    for array in arrays:
        selected = array[tuple(index)]
        scheduled = selected[selected != -1]
        elements |= set(scheduled)
    return elements


def fill_data(data):
    penalties = [
        'streams_sessions|penalty',
        'streams_rooms|penalty',
        'streams_streams|penalty',
        'sessions_rooms|penalty',
        'abstracts'
    ]
    for sheet_name in penalties:
        data[sheet_name] = data[sheet_name].fillna(0)


def print_err(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)


def session_to_timeslots_map(sessions):
    """returns a list of the range of slots
    for each session
    """
    if len(sessions) == 0:
        return []
    cumlative_slots = sessions['Max number of talks'].cumsum().tolist()
    start_slots = [0] + cumlative_slots[:-1]
    finish_slots = start_slots[1:] + [cumlative_slots[-1]]
    return list(zip(start_slots, finish_slots))


def arreq_in_list(myarr, list_arrays):
    return next((True for elem in list_arrays if np.array_equal(elem, myarr)), False)


def unscheduled_1d(iterable):

    iterator = enumerate(iterable)
    while True:
        iterator = dropwhile(lambda x: x[1] != -1, iterator)
        empty = [index
                 for index, _ in takewhile(lambda x: x[1] == -1, iterator)]
        if empty:
            yield empty[0], len(empty)
        else:
            break


def scheduled_1d(iterable):
    iterator = enumerate(iterable)
    while True:
        iterator = dropwhile(lambda x: x[1] == -1, iterator)
        scheduled = list(takewhile(lambda x: x[1] != -1, iterator))
        if not scheduled:
            break
        while scheduled:
            index, item = scheduled[0]
            consecutive = list(takewhile(lambda x: x[1] == item, scheduled))
            length = len(consecutive)
            yield index, length
            scheduled = scheduled[length:]
