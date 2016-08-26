import os
import xml.etree.ElementTree as xml

import attr
import nvector
import numpy

wgs84 = nvector.FrameE(name='WGS84')


@attr.s(slots=True)
class Point(object):
    lat = attr.ib()
    lng = attr.ib()
    _nv_geopoint = attr.ib(default=None, init=False, cmp=False, hash=False)
    _nv = attr.ib(default=None, init=False, cmp=False, hash=False)

    @property
    def nv_geopoint(self):
        if not self._nv_geopoint:
            self._nv_geopoint = wgs84.GeoPoint(latitude=self.lat, longitude=self.lng, degrees=True)
        return self._nv_geopoint

    @property
    def nv(self):
        if not self._nv:
            self._nv = self.nv_geopoint.to_nvector()
        return self._nv


@attr.s
class Path(object):
    id = attr.ib()
    # name = attr.ib()
    dir_path = attr.ib()
    route_points = attr.ib(default=None, init=False)

    async def process_upload(self, upload_file):
        os.mkdir(self.dir_path)
        with open(os.path.join(self.dir_path, 'upload.gpx'), 'wb') as f:
            f.write(upload_file)
        self.route_points = gpx_get_points(upload_file)
        self.reset_processed()
        await self.process()

    def reset_processed(self):
        pass

    async def process(self):
        pass


gpx_ns = {
    'gpx11': 'http://www.topografix.com/GPX/1/1',
}


def gpx_get_points(gpx):
    doc = xml.fromstring(gpx)
    trkpts = doc.findall('./gpx11:trk/gpx11:trkseg/gpx11:trkpt', gpx_ns)
    points = [Point(trkpt.attrib['lat'], trkpt.attrib['lon']) for trkpt in trkpts]
    return points


def pairs(items):
    return zip(items[:-1], items[1:])


def find_closest_point_pair(points, to_point, req_min_dist=20, stop_after_dist=100):
    tpn = to_point.nv.normal
    min_distance = None
    min_point_pair = None
    for point1, point2 in pairs(points):
        p1 = point1.nv.normal
        p2 = point2.nv.normal
        c12 = numpy.cross(p1, p2, axis=0)
        ctp = numpy.cross(tpn, c12, axis=0)
        c = nvector.unit(numpy.cross(ctp, c12, axis=0)).reshape((3, ))

        p1h = p1.reshape((3, ))
        p2h = p2.reshape((3, ))
        dp1p2 = nvector.deg(numpy.arccos(numpy.dot(p1h, p2h)))

        sutable_c = None
        for co in (c, 0-c):
            dp1co = nvector.deg(numpy.arccos(numpy.dot(p1h, co)))
            dp2co = nvector.deg(numpy.arccos(numpy.dot(p2h, co)))
            if abs(dp1co + dp2co - dp1p2) < 0.000001:
                sutable_c = co
                break

        if sutable_c is not None:
            c_geopoint = wgs84.Nvector(sutable_c.reshape((3, 1))).to_geo_point()
            distance = to_point.nv_geopoint.distance_and_azimuth(c_geopoint)[0]
        else:
            distance = min((to_point.nv_geopoint.distance_and_azimuth(p)[0] for p in (point1.nv_geopoint, point2.nv_geopoint)))
        if min_distance is None or distance < min_distance:
            min_distance = distance
            min_point_pair = (point1, point2)

        if min_distance < req_min_dist and distance > stop_after_dist:
            break

    return min_point_pair

