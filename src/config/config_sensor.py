import config

CONFIG_STATIONS_NAME = 'scada_stations'


def _get_all_sensors():
    config_data = config.get(CONFIG_STATIONS_NAME)
    res = []
    for item in config_data:
        c_list = item.get('children', [])
        for c in c_list:
            tag_protocol = c.get('protocol', 'H')
            tag_producer = c.get('producer', '')
            sid = c.get('zbid')
            if sid is not None:
                sid = '%s_%s' % (tag_protocol, sid)
                if tag_producer is not None and tag_producer != '':
                    sid += ('_' + tag_producer)
                c['sid'] = sid
                res.append(c)
    return res


def get_by_zbid(_id):
    all = _get_all_sensors()
    for item in all:
        if item.get('zbid') == _id:
            return item
    return None


def get_by_sid(_id):
    all = _get_all_sensors()
    for item in all:
        if item.get('sid') == _id:
            return item
    return None


def get_all():
    return _get_all_sensors()
