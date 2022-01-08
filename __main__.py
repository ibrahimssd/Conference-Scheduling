#!/usr/bin/env python3
import argparse
import pandas as pd
from conference_scheduling.checks import check_data
from conference_scheduling.utils import fill_data, print_err
from conference_scheduling.scheduler import (
    StreamsScheduler,
    AbstractsScheduler,
)
from conference_scheduling.heuristics import (
    greedy_hill_climbing,
    simulated_annealing,
    slot_tabu_search,
    steady_state_genetic_algorithm,
    streams_population,
)
from conference_scheduling.exceptions import IncompatibleDimensionsError
from conference_scheduling.config import (
    DEFAULT_INPUT_FILE,
    DEFAULT_OUTPUT_FILE,
    DEFAULT_MAXITERS,
    DEFAULT_MINSCORE,
    SOLUTION_STREAMS_SHEET,
    SOLUTION_ABSTRACTS_SHEET,
)
from conference_scheduling.utils import write_schedule


def main():
    parser = argparse.ArgumentParser(
        description='Generate a schedule for a conference.')
    parser.add_argument('-i', '--input', type=str,
                        default=DEFAULT_INPUT_FILE,
                        help=(f'The conference spreadsheet.'
                              f' Default: "{DEFAULT_INPUT_FILE}"'))
    parser.add_argument('-o', '--output', type=str,
                        default=DEFAULT_OUTPUT_FILE,
                        help=(f'The schedule output path.'
                              f' Default: "{DEFAULT_OUTPUT_FILE}"'))
    parser.add_argument('-s', '--saved', type=str,
                        help='Continue from a previously generatred \
                                   solution, or a custom starting point.')
    parser.add_argument('-m', '--maxiters', type=int,
                        default=DEFAULT_MAXITERS,
                        help='Maximum number of iterations.')
    parser.add_argument('-f', '--minscore', type=int,
                        default=DEFAULT_MINSCORE,
                        help="Minimum value of the Objective function")
    parser.add_argument('-w', '--weights', type=float,
                        metavar='W', nargs=12,
                        help="""Provide weights for the penalties as list of numbers in the following order:
        1. Parallel Streams
        2. Number of rooms per stream
        3. Number of rooms per stream with surrogate
        4. Streams vs Sessions
        5. Streams vs Rooms
        6. Sessions vs Rooms
        7. Streams vs Streams
        8. Unscheduled abstracts
        9. Misordered abstracts
        10. Abstracts vs Sessions
        11. Abstracts vs Abstracts
        12. Consecutive sessions
        """,
                        default=[1, 10, 1, 100, 1, 10, 1,
                                 10000, 1000, 100, 10, 1])

    args = parser.parse_args()

    input_data = pd.read_excel(args.input, sheet_name=None)
    fill_data(input_data)
    check_data(input_data)

    saved_streams = None
    saved_abstracts = None
    if args.saved:
        try:
            saved = pd.read_excel(args.saved,
                                  sheet_name=[
                                      SOLUTION_STREAMS_SHEET,
                                      SOLUTION_ABSTRACTS_SHEET
                                  ])
            saved_streams = saved[SOLUTION_STREAMS_SHEET]
            saved_abstracts = saved[SOLUTION_ABSTRACTS_SHEET]
        except KeyError:
            print_err(f"The provided starting solution file: {args.saved}"
                      f" should contain both of the following sheets:"
                      f" '{SOLUTION_STREAMS_SHEET}' and"
                      f" '{SOLUTION_ABSTRACTS_SHEET}'")
            exit(1)
        except IncompatibleDimensionsError:
            print_err("The dimensions of the provided"
                      " streams saved solution"
                      " are not compatible with the instance data.")
            exit(1)

    print('Streams:')
    streams_scheduler = StreamsScheduler(input_data,
                                         args.weights,
                                         initial_streams=saved_streams)
    if saved_streams is None:
        streams_scheduler.initialize()
    print(f"Initial score: {streams_scheduler.score}")
    streams_scheduler.improve(steady_state_genetic_algorithm,
                              streams_population(input_data, 40),
                              report_period=max(1, args.maxiters//10),
                              max_iters=args.maxiters)
    print(f"Final score: {streams_scheduler.score}")

    print('Abstracts:')
    abstracts_scheduler = AbstractsScheduler(input_data, args.weights,
                                             streams_scheduler.solution,
                                             initial_abstracts=saved_abstracts)
    if saved_abstracts is None:
        abstracts_scheduler.initialize()
    print(f"Initial score: {abstracts_scheduler.score}")
    abstracts_scheduler.improve(slot_tabu_search,
                                explore_size=150,
                                items_length=250,
                                pos_length=100,
                                idle_threshold=0.1,
                                report_period=max(1, args.maxiters//10),
                                max_iters=args.maxiters)
    print(f"Final score: {abstracts_scheduler.score}")

    write_schedule(args.output,
                   streams_scheduler, abstracts_scheduler,
                   input_data['abstracts'],
                   input_data['streams'],
                   input_data['sessions'],
                   input_data['rooms'])


if __name__ == '__main__':
    main()
