from abc import ABC, abstractmethod
from itertools import islice
import numpy as np
from ..operators import apply_changes


class AcceptanceCondition(ABC):
    @abstractmethod
    def acceptable(self, solution, changes, delta):
        raise NotImplementedError

    @abstractmethod
    def accept(self, solution, changes, delta):
        raise NotImplementedError

    @abstractmethod
    def reject(self):
        raise NotImplementedError


def local_search(solution, _evaluate, partial_evaluate,
                 neighbourhood,
                 acceptance_condition,
                 report_period=None,
                 idle_threshold=None,
                 explore_size=30,
                 min_iters=200,
                 max_iters=1000):
    """Implements a general tabu search heuritstic
    that is independent of the specific TabuList
    """
    if idle_threshold is None:
        idle_threshold = 1

    # the solution is modified in place to get new solutions
    # make copy as to not alter the original solution
    current_solution = np.copy(solution)
    current_delta = 0

    best_solution = current_solution
    best_delta = 0

    i = 0
    idle = 0

    while (not (i > min_iters and idle > idle_threshold*i)) and i < max_iters:
        neighbours = ((changes,
                       partial_evaluate(current_solution, changes))
                      for changes in neighbourhood(current_solution))

        acceptable = ((changes, delta)
                      for changes, delta in islice(neighbours, explore_size)
                      if acceptance_condition.acceptable(current_solution,
                                                         changes,
                                                         delta))

        best_neighbour = min(acceptable,
                             key=lambda neighbour: neighbour[1],
                             default=None)

        if best_neighbour is not None:
            # accept the best neighbour
            changes, delta = best_neighbour
            if delta < 0:
                idle = 0
            else:
                idle += 1

            acceptance_condition.accept(current_solution, changes, delta)
            apply_changes(current_solution, changes)
            current_delta += delta

            # save the best solution
            if current_delta < best_delta:
                best_solution = np.copy(current_solution)
                best_delta = current_delta
        else:
            idle += 1
            acceptance_condition.reject()

        if report_period is not None and (i+1) % report_period == 0:
            print(f"{i+1}\t\t{-current_delta}")

        i += 1

    return best_solution
