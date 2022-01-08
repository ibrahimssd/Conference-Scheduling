from .local_search import local_search, AcceptanceCondition


class GreedyHillClimbing(AcceptanceCondition):
    def __init__(self):
        self._delta = 0

    def acceptable(self, _solution, _changes, delta):
        return delta < self._delta

    def accept(self, _soluiton, _changes, delta):
        self._delta = delta

    def reject(self):
        pass


def greedy_hill_climbing(solution, evaluate, partial_evaluate,
                         neighbourhood,
                         report_period=None,
                         idle_threshold=0.20,
                         min_iters=200,
                         max_iters=500):
    condition = GreedyHillClimbing()
    return local_search(solution, evaluate, partial_evaluate,
                        neighbourhood,
                        condition,
                        report_period=report_period,
                        idle_threshold=idle_threshold,
                        explore_size=1,
                        min_iters=min_iters,
                        max_iters=max_iters)
