from functools import reduce

from .utils import print_err


def check_data(data):
    check_streams_session_streams(
        data['streams'],
        data['streams_sessions|penalty'])
    check_streams_sessions_sessions(
        data['sessions'],
        data['streams_sessions|penalty'])
    check_streams_rooms_streams(
        data['streams'],
        data['streams_rooms|penalty'])
    check_streams_rooms_rooms(
        data['rooms'],
        data['streams_rooms|penalty'])
    check_sessions_rooms_rooms(
        data['rooms'],
        data['sessions_rooms|penalty'])
    check_sessions_rooms_sessions(
        data['sessions'],
        data['sessions_rooms|penalty'])
    check_abstract_streams(
        data['abstracts'],
        data['streams'])
    check_abstracts_sessions(
        data['sessions'],
        data['abstracts'])


def _find_missing_or_unknown_items(all_items, to_check,
                                   missing_header=None,
                                   missing_message=None,
                                   unknown_header=None,
                                   unknown_message=None,
                                   ok_message=None,
                                   labels=lambda items: items,
                                   raise_=True):
    all_items, to_check = set(all_items), set(to_check)
    status, result = 'OK', None
    header, message = None, None

    if len(to_check) < len(all_items):
        status = 'MISSING_ITEMS'
        result = all_items - to_check
        header = missing_header
        message = missing_header
    elif len(to_check) > len(all_items):
        status = 'UNKNOWN_ITEMS'
        result = to_check - all_items
        header = unknown_header
        message = unknown_message

    if status == 'OK':
        if ok_message:
            print(ok_message)
        return status
    else:
        if header:
            print_err(header)
            print_err('\n'.join(f'\t{i+1}- {item}'
                                for i, item in enumerate(labels(result))))
        if message:
            print(message)

        if raise_:
            raise ValueError(message)
        else:
            return 'UNKNOWN_ITEMS', result


def check_streams_session_streams(streams, sessions_streams_penalty):
    _find_missing_or_unknown_items(
        streams.loc[:, 'Streams'],
        sessions_streams_penalty.index,
        unknown_header='The following streams are missing from \
            "streams_sessions|penalty" sheet:',
        unknown_message='Missing streams in the \
            "streams_sessions|penalty" sheet',
        missing_header='The following streams in "streams_sessions|penalty" \
            are unknown:',
        missing_message='Found unknown streams in the \
            "streams_sessions|penalty" sheet',
        ok_message='✔ All streams in sessions_streams penalties are valid')


def check_streams_sessions_sessions(sessions, streams_sessions_penalty):
    _find_missing_or_unknown_items(
        sessions.loc[:, 'Sessions'],
        streams_sessions_penalty.columns[1:],
        unknown_header='The following sessions are missing from \
            "streams_sessions|penalty" sheet:',
        unknown_message='Missing sessions in the \
            "streams_sessions|penalty" sheet',
        missing_header='The following sessions in "streams_sessions|penalty" \
            are unknown:',
        missing_message='Found unknown sessions in the \
            "streams_sessions|penalty" sheet',
        ok_message='✔ All sessions in streams_sessions penalties are valid')


def check_streams_rooms_streams(streams, rooms_streams_penalty):
    _find_missing_or_unknown_items(
        streams.loc[:, 'Streams'],
        rooms_streams_penalty.index,
        unknown_header='The following streams are missing from \
            "streams_rooms|penalty" sheet:',
        unknown_message='Missing streams in the \
            "streams_rooms|penalty" sheet',
        missing_header='The following streams in "streams_rooms|penalty" \
            are unknown:',
        missing_message='Found unknown streams in the \
            "streams_rooms|penalty" sheet',
        ok_message='✔ All streams in streams_rooms penalties are valid')


def check_streams_rooms_rooms(rooms, streams_rooms_penalty):
    _find_missing_or_unknown_items(
        rooms.loc[:, 'Rooms'],
        streams_rooms_penalty.columns[1:],
        unknown_header='The following rooms are missing from \
            "streams_rooms|penalty" sheet:',
        unknown_message='Missing rooms in the \
            "streams_rooms|penalty" sheet',
        missing_header='The following rooms in "streams_rooms|penalty" \
            are unknown:',
        missing_message='Found unknown rooms in the \
            "streams_rooms|penalty" sheet',
        ok_message='✔ All rooms in streams_rooms penalties are valid')


def check_sessions_rooms_sessions(sessions, sessions_rooms_penalty):
    _find_missing_or_unknown_items(
        sessions.loc[:, 'Sessions'],
        sessions_rooms_penalty.index,
        unknown_header='The following sessions are missing from \
            "sessions_rooms|penalty" sheet:',
        unknown_message='Missing sessions in the \
            "sessions_rooms|penalty" sheet',
        missing_header='The following sessions in "sessions_rooms|penalty" \
            are unknown:',
        missing_message='Found unknown sessions in the \
            "sessions_rooms|penalty" sheet',
        ok_message='✔ All sessions in sessions_rooms penalties are valid')


def check_sessions_rooms_rooms(rooms, sessions_rooms_penalty):
    _find_missing_or_unknown_items(
        sessions_rooms_penalty.columns[1:],
        rooms.loc[:, 'Rooms'],
        unknown_header='The following rooms are missing from \
            "sessions_rooms|penalty" sheet:',
        unknown_message='Missing rooms in the \
            "sessions_rooms|penalty" sheet',
        missing_header='The following rooms in "sessions_rooms|penalty" \
            are unknown:',
        missing_message='Found unknown rooms in the \
            "sessions_rooms|penalty" sheet',
        ok_message='✔ All rooms in sessions_rooms penalties are valid')


def check_abstract_streams(abstracts, streams):
    res = _find_missing_or_unknown_items(
        streams.loc[:, 'Streams'],
        abstracts.loc[:, 'Stream'],
        unknown_header='The following streams are not assigned to any \
            abstracts in the "abstracts" sheet:',
        unknown_message='Found streams without any assigned abstracts',
        missing_header='The folowing abstracts are assigned to \
            an unknown stream:',
        missing_message='Found unknown streams in the "abstracts" sheet',
        ok_message='✔ All abstracts are assigned to valid streams',
        labels=__faulty_abstracts_labels(abstracts),
        # since empty streams are not necessarily an error
        # only raise an error for unkown streams
        raise_=False)
    if res[0] == 'UNKNOWN_ITEMS':
        raise ValueError('Found unknown streams in the "abstracts" sheet')


def check_abstracts_sessions(sessions, abstracts):
    all_sessions = sessions.loc[:, 'Sessions']
    abstracts_sessions = abstracts.columns[4:4+len(all_sessions)]
    _find_missing_or_unknown_items(
        all_sessions,
        abstracts_sessions,
        unknown_header='The following sessions are missing from \
            "abstracts" sheet:',
        unknown_message='Missing sessions in the \
            "abstracts" sheet',
        missing_header='The following sessions in "abstracts" \
            are unknown:',
        missing_message='Found unknown sessions in the \
            "abstracts" sheet',
        ok_message='✔ All sessions in "abstracts" are valid')


def __faulty_abstracts_labels(abstracts):
    def labels(streams):
        refs = [list(abstracts.loc[abstracts['Stream'] == stream, 'Reference'])
                for stream in streams]
        return reduce(lambda flat, next_: flat + next_, refs, [])
    return labels
