from Moving_avg import MovingAvg


def test_init():
    process = MovingAvg()

    return process


def test_process():
    process = test_init()
    process.process()