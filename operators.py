"""
The operators that define the local neighbourhood of a schedule
"""


def swap_two_slots(solution, timeslots, rooms):
    items = solution[(timeslots[::-1], rooms[::-1])]
    return (items, timeslots, rooms)


def swap_abstracts(solution,
                   start, room,
                   other_start, other_room,
                   num_timeslots):
    abstarct = solution[start, room]
    other = solution[other_start, other_room]
    abstracts = [other] * num_timeslots
    abstracts.extend([abstarct] * num_timeslots)

    stop = start + num_timeslots
    other_stop = other_start + num_timeslots
    timeslots = list(range(start, stop))
    timeslots.extend(range(other_start, other_stop))

    rooms = [room] * num_timeslots
    rooms.extend([other_room] * num_timeslots)

    return (abstracts, timeslots, rooms)


def schedule_in_slot(item, timeslot, room):
    return ([item], [timeslot], [room])


def unschedule_slot(timeslot, room):
    return schedule_in_slot(-1, timeslot, room)


def schedule_in_slots(item, timeslots, rooms):
    num_slots = len(timeslots)
    return ([item]*num_slots, timeslots, rooms)


def unschedule_slots(timeslots, rooms):
    return schedule_in_slots(-1, timeslots, rooms)
