import numpy as np
from scipy.interpolate import interp1d


def interpolate_to_common_timestamps(*args):
    ts_data_pairs = list(zip(*[iter(args)] * 2))

    for pair in ts_data_pairs:
        assert len(pair[0]) == len(
            pair[1]
        ), "Data and timestamps must be the same length"
        assert len(pair[0]) == len(np.unique(pair[0])), "Timestamps must be unique"
        assert len(pair[0]) > 1, "Must have more than one timestamp"
        assert np.min(pair[0]) >= 0, "Timestamps must be positive"

    assert len(ts_data_pairs) > 1, "Must have more than one data source"

    common_ts = np.unique(np.concatenate([ts for ts, _ in ts_data_pairs]))

    assert len(common_ts) > 1, "Must have more than one timestamp"

    interp_funcs = []
    interp_data = []
    for ts, data in ts_data_pairs:
        # Undecided ????
        fill_value = data[-1]
        # fill_value = 'extrapolate'
        interp = interp1d(
            ts, data, kind="linear", fill_value=fill_value, bounds_error=False
        )
        interp_funcs.append(interp)
        interp_data.append(interp(common_ts))

    return common_ts, interp_funcs, interp_data


def interpolate_to_common_timestamps2(devices, detectors):
    """
    For each detector, interpolate the device data to the detector timestamps.
    If a device timestamp is outside the range of the detector timestamps,
    then the device data is extrapolated to use the last value.

    Parameters
    ----------
    devices : dict
        Dictionary of device data, with device name as key and a tuple of timestamps and values as value.
    detectors : dict
        Dictionary of detector data, with detector name as key and a tuple of timestamps and values as value.

    """

    # dev_timestamps = np.unique(np.concatenate([ts for ts, _ in devices.values()]))

    final_data = dict()

    for det, (det_ts, det_data) in detectors.items():
        if len(det_ts) == 0:
            continue
        assert len(det_ts) == len(
            det_data
        ), "Data and timestamps must be the same length"
        assert len(det_ts) == len(np.unique(det_ts)), "Timestamps must be unique"
        assert np.min(det_ts) >= 0, "Timestamps must be positive"

        for dev, (dev_ts, dev_data) in devices.items():
            if len(dev_ts) == 0:
                continue
            assert len(dev_ts) == len(
                dev_data
            ), "Data and timestamps must be the same length"
            assert len(dev_ts) == len(np.unique(dev_ts)), "Timestamps must be unique"
            assert np.min(dev_ts) >= 0, "Timestamps must be positive"

            if len(dev_ts) == 1:
                interp_data = np.repeat(dev_data, len(det_ts))
            else:
                fill_value = (dev_data[0], dev_data[-1])
                interp = interp1d(
                    dev_ts,
                    dev_data,
                    kind="linear",
                    fill_value=fill_value,
                    bounds_error=False,
                )
                interp_data = interp(det_ts)

            final_data[det] = {}
            final_data[det]["values"] = det_data
            final_data[det][dev] = interp_data

    return final_data


if __name__ == "__main__":
    dev_data = np.array([1, 2, 3, 4, 5])
    dev_ts = np.array([0, 1, 2, 3, 4])

    det_data = np.array([10, 20, 30, 40, 50])
    det_ts = np.array([0, 1, 2, 3, 4])

    common_ts, interp_funcs, interp_data = interpolate_to_common_timestamps(
        dev_ts, dev_data, det_ts, det_data
    )

    final_data = interpolate_to_common_timestamps2(
        {"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)}
    )

    print(final_data[("dev", "det")][1])
    print(interp_data[0])

    assert np.allclose(final_data[("dev", "det")][1], interp_data[1])
