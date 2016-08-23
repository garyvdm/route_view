import attr


@attr.s(slots=True)
class Point(object):
    lat = attr.ib()
    lng = attr.ib()


