from collections import deque
from .local_search import local_search, AcceptanceCondition
from ..operators import apply_changes
from ..utils import arreq_in_list


class SlotTabuList(AcceptanceCondition):
    def __init__(self, items_length, pos_length):
        self._delta = 0
        self._schedule_item_list = deque(maxlen=items_length)
        self._schedule_pos_list = deque(maxlen=pos_length)

    def acceptable(self, solution, changes, delta):
        new_items, timeslots, rooms = changes
        old_items = solution[(timeslots, rooms)]
        items = set(old_items) | set(new_items)
        positions = set(zip(timeslots, rooms))
        return (delta < self._delta
                or (items.isdisjoint(self._schedule_item_list)
                    and positions.isdisjoint(self._schedule_pos_list)))

    def accept(self, solution, changes, delta):
        self._delta = delta
        new_items, timeslots, rooms, = changes
        old_items = solution[(timeslots, rooms)]
        self._schedule_item_list.extend([i for i in new_items if i != -1])
        self._schedule_item_list.extend([i for i in old_items if i != -1])
        self._schedule_pos_list.extend(zip(timeslots, rooms))

    def reject(self):
        pass


def slot_tabu_search(solution, evalute, partial_evaluate,
                     neighbourhood,
                     items_length=20,
                     pos_length=20,
                     explore_size=20,
                     report_period=None,
                     idle_threshold=None,
                     min_iters=500,
                     max_iters=1000):
    """A Tabu search heuristic that remembers
    the positions and items modified from recent
    schedules
    """
    condition = SlotTabuList(items_length, pos_length)
    return local_search(
        solution, evalute, partial_evaluate,
        neighbourhood,
        condition,
        report_period=report_period,
        idle_threshold=idle_threshold,
        explore_size=explore_size,
        min_iters=min_iters,
        max_iters=max_iters)


class FullTabuList(AcceptanceCondition):
    def __init__(self, length):
        self._delta = 0
        self._solution_list = deque(maxlen=length)

    def acceptable(self, solution, changes, delta):
        new_solution = apply_changes(solution, changes, inplace=False)
        return (delta < self._delta
                or not arreq_in_list(new_solution, self._solution_list))

    def accept(self, solution, changes, delta):
        self._delta = delta
        new_solution = apply_changes(solution, changes, inplace=False)
        self._solution_list.append(new_solution)

    def reject(self):
        pass


def full_tabu_search(solution, evalute, partial_evaluate,
                     neighbourhood,
                     length=100,
                     explore_size=20,
                     report_period=None,
                     idle_threshold=None,
                     min_iters=500,
                     max_iters=1000):
    """A Tabu search heuristic that remembers
    recent schedules
    """
    condition = FullTabuList(length)
    return local_search(
        solution, evalute, partial_evaluate,
        neighbourhood,
        condition,
        report_period=report_period,
        idle_threshold=idle_threshold,
        explore_size=explore_size,
        min_iters=min_iters,
        max_iters=max_iters)
