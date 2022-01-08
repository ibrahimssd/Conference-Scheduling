import random
import numpy as np
from .greedy_hc import greedy_hill_climbing
from ..operators import apply_changes


def steady_state_genetic_algorithm(solution, evaluate, partial_evaluate,
                                   neighbourhood,
                                   population,
                                   report_period=None,
                                   crossover_prob=0.50,
                                   mutation_prob=0.90,
                                   min_iters=50,
                                   max_iters=1000):

    def local_search(solution):
        return greedy_hill_climbing(solution, evaluate, partial_evaluate,
                                    neighbourhood,
                                    max_iters=min_iters)

    # add current solution to initial population
    population = list(population)
    population.append(solution)

    # improve and evaluate population
    population = [local_search(indiv) for indiv in population]
    scores = [evaluate(indiv) for indiv in population]

    for i in range(max_iters):

        if report_period is not None and (i+1) % report_period == 0:
            print(f'μ: {np.mean(scores)}, σ: {np.std(scores)}')

        # select parents
        index, other_index = np.argsort(scores)[:2]
        parent, other_parent = population[index], population[other_index]
        score = scores[index]

        # produce child using crossover
        changes = _crossover(parent, other_parent, crossover_prob)
        child = apply_changes(parent, changes, inplace=False)
        score += partial_evaluate(parent, changes)

        # mutate child
        if random.random() < mutation_prob:
            mutation_changes = next(neighbourhood(child))
            apply_changes(child, mutation_changes)
            score += partial_evaluate(child, mutation_changes)

        # improve chid
        child = local_search(child)

        # replace the worst individual
        worst_index = np.argmax(scores)
        population[worst_index] = child
        scores[worst_index] = score

    best_index = np.argmin(scores)
    return population[best_index]


def _crossover(_parent, other_parent, prob):
    selection = np.random.default_rng().random(other_parent.shape)
    changed = (selection >= prob).nonzero()
    items = other_parent[changed]
    return (items, *changed)


def streams_population(data, population_size=100):
    streams = list(data['streams'].index)
    streams.append(-1)
    num_sessions = len(data['sessions'].index)
    num_rooms = len(data['rooms'].index)
    shape = (num_sessions, num_rooms)

    return [np.random.default_rng().choice(streams, size=shape)
            for _ in range(population_size)]
