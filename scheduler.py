from abc import ABC, abstractmethod


class Scheduler(ABC):
    def __init__(self, input_data, weights):
        self._input_data = input_data
        self._weights = weights
        self._streams = self._input_data['streams']
        self._abstracts = self._input_data['abstracts']
        self._rooms = self._input_data['rooms']
        self._sessions = self._input_data['sessions']
        self._num_sessions = len(self._sessions.index)
        self._num_rooms = len(self._rooms.index)
        self._penalty_sheets = {
            'streams_streams': self._input_data['streams_streams|penalty'],
            'streams_sessions': self._input_data['streams_sessions|penalty'],
            'streams_rooms': self._input_data['streams_rooms|penalty'],
            'sessions_rooms': self._input_data['sessions_rooms|penalty']
        }

    def find(self, heuristic, *args, **kwargs):
        self.initialize()
        self.improve(heuristic, *args, **kwargs)

    def improve(self, heuristic, *args, **kwargs):
        self.solution = heuristic(self.solution,
                                  self._weighted_evaluate,
                                  self._weighted_partial_evaluate,
                                  self.neighbourhood,
                                  *args,
                                  **kwargs)

    @property
    def score(self):
        return self._weighted_evaluate(self.solution)

    @property
    def detailed_score(self):
        return self._evaluate(self.solution)

    @property
    def violations(self):
        return self._evaluate(self.solution, violations=True)

    def _weighted_evaluate(self, solution):
        return self._weighted_penalty(
            self._evaluate(solution))

    def _weighted_partial_evaluate(self, solution, changes):
        return self._weighted_penalty(
            self._partial_evaluate(solution, changes))

    @abstractmethod
    def initialize(self):
        raise NotImplementedError

    @abstractmethod
    def _weighted_penalty(self, penalties):
        raise NotImplementedError

    @abstractmethod
    def _evaluate(self, solution, violations=False):
        raise NotImplementedError

    @abstractmethod
    def _partial_evaluate(self, solution, changes):
        raise NotImplementedError

    @abstractmethod
    def neighbourhood(self, solution):
        raise NotImplementedError

    @property
    @abstractmethod
    def solution(self):
        raise NotImplementedError

    @solution.setter
    def solution(self, soltuion):
        raise NotImplementedError
