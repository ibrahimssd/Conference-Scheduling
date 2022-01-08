import numpy as np

from .scheduler import Scheduler
from ..operators import abstracts_solution_neighbourhood
from ..penalties import evaluate_abstracts, partial_evaluate_abstracts
from ..utils import session_to_timeslots_map
from ..exceptions import IncompatibleDimensionsError


class AbstractsScheduler(Scheduler):
    def __init__(self, input_data, weights,
                 streams_solution,
                 initial_abstracts=None):
        super().__init__(input_data, weights)
        self.streams_solution = streams_solution
        self._num_slots = self._sessions['Max number of talks'].sum()

        self.abstract_solution = None
        if (initial_abstracts is not None
                and initial_abstracts.shape == (self._num_slots, self._num_rooms)):
            self.abstract_solution = np.full_like(initial_abstracts, -1)
            scheduled = initial_abstracts.notna()
            abstracts_map = dict(zip(
                list(self._abstracts["Reference"]),
                list(self._abstracts.index)
            ))
            for slot in range(self._num_slots):
                for room in range(self._num_rooms):
                    if scheduled.iloc[slot, room]:
                        abstract = initial_abstracts.iloc[slot, room]
                        self.abstract_solution[slot, room] = (
                            abstracts_map[abstract]
                        )
        elif initial_abstracts is not None:
            raise IncompatibleDimensionsError

    def initialize(self):
        self.abstract_solution = initial_solution(self.streams_solution,
                                                  self._abstracts,
                                                  self._streams,
                                                  self._sessions)

    def _weighted_penalty(self, penalties):
        return (self._weights[7] * penalties.scheduled
                + self._weights[8] * penalties.order
                + self._weights[9] * penalties.sessions
                + self._weights[10] * penalties.conflicts)

    def _evaluate(self, solution, violations=False):
        return evaluate_abstracts(solution,
                                  self.streams_solution,
                                  self._streams,
                                  self._abstracts,
                                  self._sessions,
                                  violations=violations)

    def _partial_evaluate(self, solution, changes):
        return partial_evaluate_abstracts(solution, changes,
                                          self.streams_solution,
                                          self._abstracts,
                                          self._sessions)

    def neighbourhood(self, solution):
        return abstracts_solution_neighbourhood(solution,
                                                self.streams_solution,
                                                self._streams,
                                                self._abstracts,
                                                self._sessions)

    @property
    def solution(self):
        return self.abstract_solution

    @solution.setter
    def solution(self, solution):
        self.abstract_solution = solution


def initial_solution(streams_solution, abstracts, streams, sessions):
    total_timeslots = sessions['Max number of talks'].sum()
    _num_sessions, num_rooms = streams_solution.shape
    session_to_timeslots = session_to_timeslots_map(sessions)
    solution = np.full((total_timeslots, num_rooms), -1, dtype=np.int)

    for stream in streams.index:
        assigned_sessions = [(session, session_to_timeslots[session], room)
                             for session, room in np.argwhere(
                                 streams_solution == stream)]
        stream_name = streams.loc[stream, 'Streams']
        stream_abstracts = abstracts.loc[abstracts['Stream'] == stream_name,
                                         :].sort_values('Order')

        # schedule the abstracts of each stream in order in the first
        # available session, always scheduling at the begining
        # of the session if possible
        for abstract in stream_abstracts.index:
            required_timeslots = stream_abstracts.loc[abstract,
                                                      'Required Timeslots']

            for _, session_data in enumerate(assigned_sessions):
                session, timeslot_range, room_index = session_data
                session_start, session_end = timeslot_range
                timeslots = solution[session_start:session_end, room_index]
                first_empty_slot = np.argmax(timeslots == -1)
                max_number_of_talks = sessions.loc[session,
                                                   'Max number of talks']
                # schedule the abstract for the full session
                # if the time slots match exactly, and the session is empty
                if (max_number_of_talks == required_timeslots
                        and solution[session_start, room_index] == -1):
                    solution[session_start:session_end, room_index] = abstract
                    break
                # or if there are more time slots than required,
                # and the session is not full,
                # and has enough timeslots for the abstracts
                last_slot = first_empty_slot + required_timeslots
                if (max_number_of_talks > required_timeslots
                        and timeslots[-1] == -1
                        and last_slot <= session_end):
                    start = session_start + first_empty_slot
                    end = start + required_timeslots
                    solution[start:end, room_index] = abstract
                    break
                # if the abstract cannot scheduled move to the next slot
    return solution
