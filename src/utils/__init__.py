# coding=utf-8


def pim(t, PADDING, INTERVAL):
    mt = t - t % 60
    padding = int(mt + PADDING - 1 - (mt + PADDING - 1) % PADDING)
    index = PADDING / INTERVAL - (padding - mt) / INTERVAL
    index = int(index - 1)
    metric = int(mt + INTERVAL - 1 - (mt + INTERVAL - 1) % INTERVAL)
    return padding, index, metric


def to_int(s):
    """
    安全的转换为整数，如果失败，返回None
    :param s:
    :return:
    """
    try:
        return int(s)
    except ValueError as err:
        return None


def to_float(s):
    """
    安全的转换为浮点数，如果失败，返回None
    :param s:
    :return:
    """
    try:
        return float(s)
    except ValueError as err:
        return None
