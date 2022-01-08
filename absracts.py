from collections import deque
import numpy as np
from ..utils import (
    unique_scheduled_elements,
    scheduled_1d
)


def evaluate_abstracts_sessions(solution, streams_solution,
                                timeblock_to_timeslots,
                                sessions_df,
                                abstracts_df,
                                violations=False):
    penalty = 0
    violations_list = []
    num_timeblocks = streams_solution.shape[0]

    for timeblock in range(num_timeblocks):
        start, end = timeblock_to_timeslots[timeblock]
        abstracts = set(solution[start:end, :].flat)
        abstracts.discard(-1)
        for abstract in abstracts:
            timeblock_name = sessions_df.at[timeblock, 'Sessions']
            abstract_penalty = abstracts_df.loc[abstract, timeblock_name]
            penalty += abstract_penalty
            if violations and abstract_penalty != 0:
                violations_list.append((abstract, timeblock, abstract_penalty))
    if violations:
        return violations_list
    return penalty


def partial_abstracts_sessions(solution, new_solution, changed,
                               timeslot_to_timeblock,
                               sessions_df,
                               abstracts_df):
    partial_penalty = 0
    accounted_for_old = {-1}
    accounted_for_new = {-1}
    for timeslot, room in zip(*changed):
        timeblock = timeslot_to_timeblock[timeslot]
        session_name = sessions_df.at[timeblock, 'Sessions']
        old_abstract = solution[timeslot, room]
        new_abstract = new_solution[timeslot, room]
        if old_abstract not in accounted_for_old:
            partial_penalty -= abstracts_df.loc[old_abstract, session_name]
            accounted_for_old.add(old_abstract)
        if new_abstract not in accounted_for_new:
            partial_penalty += abstracts_df.loc[new_abstract, session_name]
            accounted_for_new.add(new_abstract)

    return partial_penalty


def evaluate_abstracts_order(solution, streams_solution, streams,
                             abstracts_df,
                             session_to_timeslots,
                             violations=False):
    penalty = 0
    violations_list = []
    for stream in streams:
        timeblocks, rooms = np.nonzero(streams_solution == stream)
        stream_orders = deque()
        for timeblock in set(timeblocks):
            start, end = session_to_timeslots[timeblock]
            assoc_rooms = rooms[np.flatnonzero(timeblocks == timeblock)]
            timeblock_orders = [[] for _timeslot in range(start, end)]
            for room in assoc_rooms:
                abstracts = solution[start:end, room]
                for timeslot, _length in scheduled_1d(abstracts):
                    abstract = solution[start + timeslot, room]
                    order = abstracts_df.at[abstract, "Order"]
                    if order != 0:
                        timeblock_orders[timeslot].append((abstract, order))
            stream_orders.extend(timeblock_orders)
        while stream_orders:
            for abstract, order in stream_orders.popleft():
                abstract_penalty = sum(1
                                       for succ_orders in stream_orders
                                       for _, succ_order in succ_orders
                                       if order > succ_order)
                penalty += abstract_penalty
                if violations and abstract_penalty > 0:
                    violations_list.append((abstract, abstract_penalty))
    if violations:
        return violations_list
    return penalty


def partial_abstracts_order(solution, new_solution, changed,
                            streams_solution,
                            abstracts_df,
                            timeslot_to_session,
                            session_to_timeslots):
    timeslots, rooms = changed
    changed_sessions = [timeslot_to_session[timeslot]
                        for timeslot in timeslots]
    changed_streams = set(streams_solution[(changed_sessions, rooms)])
    changed_streams.discard(-1)
    return (evaluate_abstracts_order(new_solution, streams_solution,
                                     changed_streams,
                                     abstracts_df,
                                     session_to_timeslots)
            - evaluate_abstracts_order(solution, streams_solution,
                                       changed_streams,
                                       abstracts_df,
                                       session_to_timeslots))


def evaluate_scheduled(solution, abstracts, violations=False):
    solution_abstracts = solution.ravel()
    num_slots = len(solution_abstracts)
    unscheduled = set(abstracts)
    slot = 0
    while unscheduled and slot < num_slots:
        abstract = solution_abstracts[slot]
        unscheduled.discard(abstract)
        slot += 1

    if violations:
        return list(unscheduled)
    return len(unscheduled)


def partial_scheduled(solution, new_solution, changed):
    changed_abstracts = unique_scheduled_elements(changed,
                                                  solution, new_solution)

    return (evaluate_scheduled(new_solution, changed_abstracts)
            - evaluate_scheduled(solution, changed_abstracts))


def evaluate_abstracts_abstracts(solution,
                                 abstracts, abstracts_df,
                                 timeslot_to_timeblock,
                                 timeblock_to_timeslots,
                                 violations=False):
    violations_list = []
    clashes_start = abstracts_df.columns.get_loc('Clash')
    abstracts_map = dict(zip(abstracts_df['Reference'], abstracts_df.index))
    clashes = abstracts_df.iloc[:, clashes_start:]
    penalty = 0
    for abstract in abstracts:
        slots = np.argwhere(solution == abstract)
        if len(slots) == 0:
            continue
        timeslot, _room = slots[0]
        timeblock = timeslot_to_timeblock[timeslot]
        start, end = timeblock_to_timeslots[timeblock]
        timeblock_abstracts = solution[start:end, :]
        for clash_ref in clashes.loc[abstract, :]:
            if clash_ref != 0:
                clash = abstracts_map[clash_ref]
                if np.any(timeblock_abstracts == clash):
                    if violations:
                        violations_list.append((abstract, clash, timeblock))
                    penalty += 1
    if violations:
        return violations_list
    return penalty


def partial_abstracts_abstracts(solution, new_solution, changed,
                                abstracts_df,
                                timeslot_to_session, session_to_timeslots):
    changed_abstracts = unique_scheduled_elements(changed,
                                                  solution, new_solution)

    return (evaluate_abstracts_abstracts(new_solution, changed_abstracts,
                                         abstracts_df,
                                         timeslot_to_session,
                                         session_to_timeslots)
            - evaluate_abstracts_abstracts(solution, changed_abstracts,
                                           abstracts_df,
                                           timeslot_to_session,
                                           session_to_timeslots))
