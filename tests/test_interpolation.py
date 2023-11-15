import numpy as np

from src.utils import interpolate_to_common_timestamps2


def test_can_return_most_simple_case():
    dev_data = np.array([1, 2, 3, 4, 5])
    dev_ts = np.array([0, 1, 2, 3, 4])

    det_data = np.array([10, 20, 30, 40, 50])
    det_ts = np.array([0, 1, 2, 3, 4])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, dev_data)


def test_can_plot_single_count_single_pos():
    dev_data = np.array([1])
    dev_ts = np.array([0])

    det_data = np.array([10])
    det_ts = np.array([1])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, dev_data)


def test_can_plot_single_count_multiple_pos():
    dev_data = np.array([1, 2, 3, 4, 5])
    dev_ts = np.array([0, 1, 2, 3, 4])

    det_data = np.array([10])
    det_ts = np.array([1])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([2]))


def test_can_plot_multiple_count_single_pos():
    dev_data = np.array([1])
    dev_ts = np.array([0])

    det_data = np.array([10, 20, 30])
    det_ts = np.array([1, 2, 3])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([1, 1, 1]))


def test_can_plot_multiple_count_multiple_pos():
    dev_data = np.array([1, 4])
    dev_ts = np.array([0, 3])

    det_data = np.array([10, 20, 30])
    det_ts = np.array([1, 2, 3])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([2, 3, 4]))


def test_can_interpolate_different_timestamps_leading_detector():
    dev_data = np.array([1, 2, 3, 4, 5])
    dev_ts = np.array([0, 1, 2, 3, 4])

    det_data = np.array([10, 20, 30, 40, 50])
    det_ts = np.array([0.5, 1.5, 2.5, 3.5, 4.5])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([1.5, 2.5, 3.5, 4.5, 5]))


def test_can_interpolate_different_timestamps_leading_device():
    dev_data = np.array([0, 1, 2, 3, 4, 5])
    dev_ts = np.array([0, 1, 2, 3, 4, 5])

    det_data = np.array([10, 20, 30, 40, 50])
    det_ts = np.array([0.5, 1.5, 2.5, 3.5, 4.5])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([0.5, 1.5, 2.5, 3.5, 4.5]))


def test_extrapolate_limits():
    dev_data = np.array([2, 3])
    dev_ts = np.array([2, 3])

    det_data = np.array([10, 20, 30, 40, 50])
    det_ts = np.array([1, 2, 3, 4, 5])

    final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

    detector_data_to_plot = final_data["det"]["values"]
    device_data_to_plot = final_data["det"]["dev"]

    assert np.allclose(detector_data_to_plot, det_data)
    assert np.allclose(device_data_to_plot, np.array([2, 2, 3, 3, 3]))


def test_basic_scan_motor_first():
    dev_dat_points = 0
    det_dat_points = 0
    for i in range(11):
        dev_data = np.array([j for j in range(dev_dat_points)])
        dev_ts = np.array([j for j in range(dev_dat_points)])

        det_data = np.array([j+1 for j in range(det_dat_points)])
        det_ts = np.array([j for j in range(det_dat_points)])

        final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

        if i == 0:  # No data
            assert not final_data
        elif i == 1:  # one motor point, no detector points
            assert not final_data
        elif i == 2:  # one motor point, one detector point
            assert np.allclose(final_data["det"]["dev"], np.array([0]))
            assert np.allclose(final_data["det"]["values"], np.array([1]))
        elif i == 3:
            assert np.allclose(final_data["det"]["dev"], np.array([0]))
            assert np.allclose(final_data["det"]["values"], np.array([1]))
        elif i == 4:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2]))
        elif i == 5:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2]))
        elif i == 6:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3]))
        elif i == 7:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3]))
        elif i == 8:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4]))
        elif i == 9:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4]))
        elif i == 10:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3, 4]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4, 5]))

        if i % 2 == 0:
            dev_dat_points += 1
        else:
            det_dat_points += 1


def test_basic_scan_detector_first():
    dev_dat_points = 0
    det_dat_points = 0
    for i in range(11):
        dev_data = np.array([j for j in range(dev_dat_points)])
        dev_ts = np.array([j for j in range(dev_dat_points)])

        det_data = np.array([j+1 for j in range(det_dat_points)])
        det_ts = np.array([j for j in range(det_dat_points)])

        final_data = interpolate_to_common_timestamps2({"dev": (dev_ts, dev_data)}, {"det": (det_ts, det_data)})

        if i == 0:  # No data
            assert not final_data
        elif i == 1:  # one detector point, no motor points
            assert not final_data
        elif i == 2:  # one detector point, one motor point
            assert np.allclose(final_data["det"]["dev"], np.array([0]))
            assert np.allclose(final_data["det"]["values"], np.array([1]))
        elif i == 3: # two detector points, one motor point
            assert np.allclose(final_data["det"]["dev"], np.array([0, 0]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2]))
        elif i == 4:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2]))
        elif i == 5:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 1]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3]))
        elif i == 6:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3]))
        elif i == 7:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 2]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4]))
        elif i == 8:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4]))
        elif i == 9:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3, 3]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4, 5]))
        elif i == 10:
            assert np.allclose(final_data["det"]["dev"], np.array([0, 1, 2, 3, 4]))
            assert np.allclose(final_data["det"]["values"], np.array([1, 2, 3, 4, 5]))

        if i % 2 == 0:
            det_dat_points += 1
        else:
            dev_dat_points += 1
