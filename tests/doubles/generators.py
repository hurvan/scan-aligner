import numpy as np
from streaming_data_types import serialise_f144, serialise_ev44, deserialise_ev44, deserialise_f144
from src.deserialiser import handle_f144_data, handle_ev44_data


def gauss_func(x, h, a, x0, var):
    return h + a * np.exp(-(x - x0) ** 2 / (2 * var))


def generate_gauss_f144_data(num_messages, source_name="f144_source_1"):
    assert num_messages % 2 == 1, "num_messages must be odd"
    vals = np.linspace(0, 10, num_messages)
    vals += np.random.normal(0, 0.01, num_messages)
    vals = vals.tolist()
    times = np.linspace(0, 1000, num_messages).astype(np.int64).tolist()
    data = []
    for val, t in zip(vals, times):
        data.append(
            serialise_f144(source_name, val, t)
        )
    return data


def generate_gauss_ev44_events(num_messages, source_name="ev44_source_1", mean=5, std=1):
    assert num_messages % 2 == 1, "num_messages must be odd"
    vals = gauss_func(np.linspace(0, 10, num_messages), 0, 100, mean, std)
    vals += np.abs(np.random.normal(0, 5, num_messages))
    vals = vals.astype(np.int64).tolist()
    times = np.linspace(0, 1000, num_messages).astype(np.int64).tolist()
    events = []
    for val, t in zip(vals, times):
        tofs = np.arange(0, val, 1).astype(np.int64).tolist()
        det_ids = [1] * len(tofs)
        events.append(
            serialise_ev44(source_name, t, [t], [0], tofs, det_ids)
        )
    return events


def generate_simple_fake_ev44_events(num_messages, source_name="ev44_source_1"):
    events = []
    for i in range(num_messages):
        tofs = [0, 1, 2, 3]
        det_ids = [1] * len(tofs)
        time_ns = i
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    return events


def generate_simple_fake_f144_data(num_messages, source_name="f144_source_1"):
    data = []
    for i in range(num_messages):
        data.append(
            serialise_f144(source_name, i, i)
        )
    return data


def generate_linear_fake_ev44_events(num_messages, source_name="ev44_source_1", start_offset=0):
    events = []
    for i in range(num_messages):
        i += start_offset
        tofs = [i for i in range(i+1)]
        det_ids = [1] * len(tofs)
        time_ns = i*2
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    return events


def generate_linear_fake_f144_data(num_messages, source_name="f144_source_1", start_offset=0):
    data = []
    for i in range(num_messages):
        i += start_offset
        data.append(
            serialise_f144(source_name, i, (i*2 + 1))
        )
    return data


def generate_simple_fake_deserialised_ev44_events(num_messages, source_name="ev44_source_1"):
    events = []
    for i in range(num_messages):
        tofs = [0, 1, 2, 3]
        det_ids = [1] * len(tofs)
        time_ns = i
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    events = [handle_ev44_data(deserialise_ev44(event)) for event in events]
    return events


def generate_simple_fake_deserialised_f144_data(num_messages, source_name="f144_source_1"):
    data = []
    for i in range(num_messages):
        data.append(
            serialise_f144(source_name, i, i)
        )
    data = [handle_f144_data(deserialise_f144(dat)) for dat in data]
    return data


def generate_linear_fake_deserialised_ev44_events(num_messages, source_name="ev44_source_1"):
    events = []
    for i in range(num_messages):
        tofs = [i for i in range(i+1)]
        det_ids = [1] * len(tofs)
        time_ns = i*2
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    events = [handle_ev44_data(deserialise_ev44(event)) for event in events]
    return events


def generate_linear_fake_deserialised_f144_data(num_messages, source_name="f144_source_1"):
    data = []
    for i in range(num_messages):
        data.append(
            serialise_f144(source_name, i, (i*2 + 1))
        )
    data = [handle_f144_data(deserialise_f144(dat)) for dat in data]
    return data


def f144_generator(val, t, source_name="f144_source_1"):
    return serialise_f144(source_name, val, t)


def ev44_generator(val, t, source_name="ev44_source_1"):
    val = max(1, val)
    tofs = [i for i in range(val)]
    det_ids = [1] * len(tofs)
    return serialise_ev44(source_name, t, [t], [0], tofs, det_ids)
