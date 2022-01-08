import math

import numpy as np
from ..utils import unique_scheduled_elements


def evaluate_consecutive_sessions(solution, streams, violations=False):
    violations_list = []
    overall_penalty = 0
    num_sessions, num_rooms = solution.shape
    for stream in streams:
        for room in range(num_rooms):
            count = 0
            cons = 0
            for session in range(num_sessions - 1):
                if solution[session][room] == stream:
                    if solution[session + 1][room] == stream:
                        cons = cons + 1
                    count = count + 1
            if solution[num_sessions - 1][room] == stream:
                count = count + 1
            penalty = count - cons - 1
            if penalty > 0:
                overall_penalty = overall_penalty + penalty
                if violations:
                    violations_list.append((stream, room, penalty))

    if violations:
        return violations_list

    return overall_penalty


def partial_consecutive_sessions(solution, new_solution, changed):
    changed_streams = unique_scheduled_elements(changed,
                                                solution, new_solution)
    return (evaluate_consecutive_sessions(new_solution, changed_streams)
            - evaluate_consecutive_sessions(solution, changed_streams))


def evaluate_streams_streams(solution, sessions,
                             streams_streams_penalty,
                             violations=False):
    violations_list = []
    penalties_sum = 0
    _num_sessions, num_rooms = solution.shape
    for sess in sessions:
        sess_streams = solution[sess, :]
        for room in range(num_rooms):
            for other_room in range(num_rooms):
                stream = sess_streams[room]
                other_stream = sess_streams[other_room]
                if stream != other_stream and stream != -1 and other_stream != -1:
                    penalty = streams_streams_penalty.iloc[sess_streams[room],
                                                           sess_streams[other_room] + 1]
                    penalties_sum += penalty
                    if violations and penalty != 0:
                        violations_list.append((stream, other_stream,
                                                sess, penalty))
    if violations:
        return violations_list
    return penalties_sum


def partial_streams_streams(solution, new_solution, changed,
                            streams_streams_penalty):
    changed_sessions = np.unique(changed[0])
    return (evaluate_streams_streams(new_solution, changed_sessions,
                                     streams_streams_penalty)
            - evaluate_streams_streams(solution, changed_sessions,
                                       streams_streams_penalty))


def evaluate_parallel_streams(solution, streams, required_sessions,
                              violations=False):
    """computes a penalty that increases exponentially with
    the number of times a stream occurs in a single session
    """
    violations_list = []
    penalty = 0
    num_sessions = solution.shape[0]
    minimum_parallel = minimum_parallel_sessions(num_sessions,
                                                 required_sessions)
    for stream in streams:
        occurrances_per_session = np.sum(solution == stream, axis=1)
        temp = np.sum(occurrances_per_session
                      * (occurrances_per_session - 1) / 2)
        stream_penalty = temp - minimum_parallel[stream]
        if violations and stream_penalty > 0:
            violations_list.append((stream, stream_penalty))
        penalty += stream_penalty
    if violations:
        return violations_list
    return penalty


def partial_parallel_streams(solution, new_solution, changed,
                             required_sessions):
    changed_streams = unique_scheduled_elements(changed,
                                                solution,
                                                new_solution)
    return (evaluate_parallel_streams(new_solution, changed_streams,
                                      required_sessions)
            - evaluate_parallel_streams(solution, changed_streams,
                                        required_sessions))


def minimum_parallel_sessions(num_sessions, required_sessions):
    """
    duplications: all sessions must have a minimum of `duplications`
       of the same stream
    extra_sessions: minimum number of sessions that must have
       more than `duplication`
    """
    duplications, extra_sessions = divmod(required_sessions, num_sessions)
    # sessions in which the stream ideally appears `duplications` times
    diff = num_sessions - extra_sessions
    return (diff * duplications * (duplications - 1)
            + extra_sessions * (duplications + 1) * (duplications)) / 2


def evaluate_number_of_rooms_per_stream(solution, streams, required_sessions,
                                        violations=False):
    """Return the total number of rooms more than the minimum taken by each stream
    """
    violations_list = []
    penalty = 0
    num_sessions = solution.shape[0]
    for stream in streams:
        num_of_rooms = _stream_num_rooms(stream, solution)
        minimum_rooms = math.ceil(required_sessions[stream] / num_sessions)
        stream_penalty = max(num_of_rooms - minimum_rooms, 0)
        penalty += stream_penalty
        if violations and stream_penalty > 0:
            violations_list.append((stream, stream_penalty))
    if violations:
        return violations_list
    return penalty


def partial_number_of_rooms_per_stream(solution, new_solution, changed,
                                       required_sessions):
    changed_streams = unique_scheduled_elements(changed,
                                                solution, new_solution)
    return (evaluate_number_of_rooms_per_stream(new_solution, changed_streams,
                                                required_sessions)
            - evaluate_number_of_rooms_per_stream(solution, changed_streams,
                                                  required_sessions))


def evaluate_streams_scheduled(solution, streams, violations=False):
    solution_streams = solution.ravel()
    num_slots = len(solution_streams)
    unscheduled = set(streams)
    slot = 0
    while unscheduled and slot < num_slots:
        stream = solution_streams[slot]
        unscheduled.discard(stream)
        slot += 1

    if violations:
        return list(unscheduled)
    return len(unscheduled)


def partial_streams_scheduled(solution, new_solution, changed):
    changed_streams = unique_scheduled_elements(changed,
                                                solution, new_solution)
    return (evaluate_streams_scheduled(new_solution, changed_streams)
            - evaluate_streams_scheduled(solution, changed_streams))


def evaluate_penalties(solution,
                       streams_sessions, streams_rooms, sessions_rooms,
                       violations=False):
    violations_lists = ([], [], [])
    penalties = [0, 0, 0]
    num_sessions, num_rooms = solution.shape
    for session in range(num_sessions):
        for room in range(num_rooms):
            stream = solution[session, room]
            if stream != -1:
                stream_session_penalty = streams_sessions.iloc[stream, session+1]
                stream_room_penalty = streams_rooms.iloc[stream, room+1]
                session_room_penalty = sessions_rooms.iloc[session, room+1]

                penalties[0] += stream_session_penalty
                penalties[1] += stream_room_penalty
                penalties[2] += session_room_penalty

                if violations:
                    if stream_session_penalty != 0:
                        violations_lists[0].append((
                            stream,
                            session,
                            stream_session_penalty
                        ))
                    if stream_room_penalty != 0:
                        violations_lists[1].append((
                            stream,
                            room,
                            stream_room_penalty
                        ))
                    if session_room_penalty != 0:
                        violations_lists[2].append((
                            session,
                            room,
                            session_room_penalty
                        ))
    if violations:
        return violations_lists
    return tuple(penalties)


def partial_penalties(solution, new_solution, changed,
                      sessions_rooms, streams_rooms, streams_sessions):
    deltas = [0, 0, 0]
    for session, room in zip(*changed):
        old_stream = solution[session, room]
        stream = new_solution[session, room]
        # remove old penalty values
        if old_stream != -1:
            deltas[0] -= streams_sessions.iloc[old_stream, session + 1]
            deltas[1] -= streams_rooms.iloc[old_stream, room + 1]
            deltas[2] -= sessions_rooms.iloc[session, room + 1]
        # add new penalty values
        if stream != -1:
            deltas[0] += streams_sessions.iloc[stream, session + 1]
            deltas[1] += streams_rooms.iloc[stream, room + 1]
            deltas[2] += sessions_rooms.iloc[session, room + 1]
    return tuple(deltas)


def _stream_num_rooms(stream, solution):
    occurrances_per_room = np.sum(solution == stream, axis=0)
    return len(np.flatnonzero(occurrances_per_room))
