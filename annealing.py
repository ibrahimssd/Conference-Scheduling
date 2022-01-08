import random
from math import exp, log as ln
from .local_search import local_search, AcceptanceCondition


class SimulatedAnnealing(AcceptanceCondition):
    def __init__(self,
                 min_delta, max_delta,
                 max_iters,
                 init_prob=0.95,
                 sat_prob=0.05):
        self._delta = 0
        self.temperature = -max_delta / ln(init_prob)
        final_temperature = -min_delta / ln(sat_prob)
        self.cooling_factor = 1 / (final_temperature * max_iters)

    def acceptable(self, _solution, _changes, delta):
        return (delta < self._delta
                or random.random() < exp(- delta / self.temperature))

    def _cool(self):
        self.temperature /= 1 + self.cooling_factor * self.temperature

    def accept(self, _soluiton, _changes, delta):
        self._delta = delta
        self._cool()

    def reject(self):
        self._cool()


def simulated_annealing(solution, evaluate, partial_evaluate,
                        neighbourhood,
                        max_delta,
                        min_delta=20,
                        init_prob=0.95,
                        sat_prob=0.05,
                        report_period=None,
                        idle_threshold=0.20,
                        min_iters=200,
                        max_iters=500):
    """Search for an optimal abstracts schedule
    in atmost `max_iters` iterations
    """
    condition = SimulatedAnnealing(min_delta, max_delta,
                                   max_iters,
                                   init_prob=init_prob,
                                   sat_prob=sat_prob)
    return local_search(solution,
                        evaluate, partial_evaluate, neighbourhood,
                        condition,
                        report_period=report_period,
                        idle_threshold=idle_threshold,
                        explore_size=1,
                        min_iters=min_iters,
                        max_iters=max_iters)
