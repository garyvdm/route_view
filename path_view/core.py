import os
import logging
import json
import functools
import asyncio
import xml.etree.ElementTree as xml

import aiohttp
import attr
import unqlite
from nvector import (
    FrameE,
    unit,
    deg,
)
from numpy import (
    cross,
    dot,
    arccos,
)

wgs84 = FrameE(name='WGS84')


def runs_in_executor(fn):

    @functools.wraps(fn)
    async def runs_in_executor_inner(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))

    return runs_in_executor_inner


@attr.s(slots=True)
class Point(object):
    lat = attr.ib()
    lng = attr.ib()
    i_nv_geopoint = attr.ib(default=None, cmp=False, hash=False, repr=False)
    i_nv = attr.ib(default=None, cmp=False, hash=False, repr=False)

    @property
    def nv_geopoint(self):
        if not self.i_nv_geopoint:
            self.i_nv_geopoint = wgs84.GeoPoint(latitude=self.lat, longitude=self.lng, degrees=True)
        return self.i_nv_geopoint

    @property
    def nv(self):
        if not self.i_nv:
            self.i_nv = self.nv_geopoint.to_nvector()
        return self.i_nv


@attr.s(slots=True)
class IndexedPoint(Point):
    index = attr.ib(default=None, )

path_meta_attrs = {'name', 'processing_complete'}
path_route_attrs = {'route_points', }


@attr.s
class Path(object):
    id = attr.ib()
    dir_path = attr.ib()
    new_pano_callback = attr.ib()
    name = attr.ib(default=None)
    process_task = attr.ib(default=None, init=False)
    data_loaded = attr.ib(default=False, init=False)
    processing_complete = attr.ib(default=False)
    route_points = attr.ib(default=None, init=False)
    panos = attr.ib(default=[], init=False)
    prefered_pano_chain = attr.ib(default={}, init=False)

    @classmethod
    @runs_in_executor
    def load(cls, id, dir_path, new_pano_callback):
        with open(os.path.join(dir_path, 'meta.json'), 'r') as f:
            meta = json.load(f)
        return Path(id, dir_path, new_pano_callback, **meta)

    @runs_in_executor
    def ensure_data_loaded(self):
        # TODO resume processing if not complete
        if not self.data_loaded:
            with open(os.path.join(self.dir_path, 'route.json'), 'r') as f:
                route = json.load(f)
            route['route_points'] = [IndexedPoint(*point, index=i) for i, point in enumerate(route['route_points'])]
            for k, v in route.items():
                setattr(self, k, v)
            with open(os.path.join(self.dir_path, 'panos.json'), 'r') as f:
                self.panos = json.load(f)
            for pano in self.panos:
                pano['point'] = Point(*pano['point'])

            self.data_loaded = True

    @runs_in_executor
    def save_metadata(self):
        meta = attr.asdict(self, filter=lambda a, v: a.name in path_meta_attrs)
        with open(os.path.join(self.dir_path, 'meta.json'), 'w') as f:
            json.dump(meta, f)

    @runs_in_executor
    def save_route(self):
        def json_encode(obj):
            if isinstance(obj, Point):
                return (obj.lat, obj.lng)
            if isinstance(obj, Path):
                return attr.asdict(obj, recurse=False, filter=lambda a, v: a.name in path_route_attrs)

        with open(os.path.join(self.dir_path, 'route.json'), 'w') as f:
            json.dump(self, f, default=json_encode)

    @runs_in_executor
    def save_panos(self):
        def json_encode(obj):
            if isinstance(obj, Point):
                return (obj.lat, obj.lng)

        # TODO save panos as they are being fetched.
        with open(os.path.join(self.dir_path, 'panos.json'), 'w') as f:
            json.dump(self.panos, f, default=json_encode)

    async def load_route_from_gpx(self, gpx):
        os.mkdir(self.dir_path)
        await self.save_metadata()
        with open(os.path.join(self.dir_path, 'upload.gpx'), 'wb') as f:
            f.write(gpx)
        await self.set_route_points(gpx_get_points(gpx))

    async def set_route_points(self, points):
        if self.process_task:
            self.process_task.cancel()
            await self.process_task
        self.route_points = points
        await self.save_route()
        await self.reset_processed()
        self.data_loaded = True
        self.processing_complete = True

    async def start_processing(self, google_api):
        self.process_task = asyncio.ensure_future(self.process(google_api))
        self.process_task.add_done_callback(self.process_task_done_callback)

    def process_task_done_callback(self, fut):
        try:
            fut.result()
        except asyncio.CancelledError:
            logging.info("Processing cancelled.")
        except Exception:
            logging.exception("Processing error: ")
        self.process_task = None

    async def reset_processed(self):
        self.panos = []
        await self.save_panos()

    async def process(self, google_api):
        # if self.panos:GoogleApi
        #     last_pano = self.panos[-1]
        #     last_point_index = self.route_points[last_pano.closest_point_pair_index]
        # else:
        #     last_pano = None
        #     last_point_index = 0
        last_pano = None
        last_point = self.route_points[0]
        last_point_index = 0

        last_save_task = None

        try:

            while True:
                if last_pano is None:
                    for point in iter_points_with_minimal_spacing([last_point] + self.route_points[last_point_index + 1:],
                                                                  spacing=10):
                        pano_data = await google_api.get_pano_ll(point)
                        if pano_data:
                            break
                    else:
                        break
                else:
                    if last_pano['id'] in self.prefered_pano_chain:
                        link_pano_id = self.prefered_pano_chain[last_pano['id']]
                    else:
                        # if last_point_index == len(points_indexed) -1 :
                        #     break

                        point1 = self.route_points[last_point_index]
                        point2 = self.route_points[last_point_index + 1]
                        yaw_to_next = deg(point1.nv_geopoint.distance_and_azimuth(point2.nv_geopoint)[1])
                        yaw_diff = lambda item: abs(deg_wrap_to_closest(item['yaw'] - yaw_to_next, 0))
                        pano_link = min(last_pano['links'], key=yaw_diff)

                        if yaw_diff(pano_link) > 20:
                            logging.debug("Yaw too different: {} {} {}".format(yaw_diff(pano_link), pano_link['yaw'], yaw_to_next))
                            link_pano_id = None
                        else:
                            link_pano_id = pano_link['panoId']

                    if link_pano_id:
                        pano_data = await google_api.get_pano_id(link_pano_id)
                    else:
                        last_pano = None
                        pano_data = None

                if pano_data:
                    location = pano_data['Location']
                    pano_point = Point(lat=float(location['lat']), lng=float(location['lng']))
                    point_pair, c_point, dist = find_closest_point_pair(self.route_points[last_point_index:], pano_point)

                    if dist > 30:
                        logging.debug("Distance {} to nearest point too great for pano: {}"
                                      .format(dist, location['panoId']))
                        last_pano = None
                    else:
                        heading = deg(point_pair[0].nv_geopoint.distance_and_azimuth(point_pair[1].nv_geopoint)[1])
                        links = [dict(panoId=link['panoId'], yaw=float(link['yawDeg']))
                                 for link in pano_data['Links']]
                        pano = dict(
                            id=location['panoId'], point=pano_point,
                            description=location['description'], links=links, i=last_point_index, heading=heading)
                        self.panos.append(pano)
                        last_pano = pano
                        last_point_index = point_pair[0].index
                        last_point = c_point
                        self.new_pano_callback(pano)
                        logging.info("{description} ({point.lat},{point.lng}) {i}".format(**pano))
                        if len(self.panos) % 100 == 0:
                            if last_save_task:
                                await asyncio.shield(last_save_task)
                            last_save_task = asyncio.ensure_future(self.save_panos())
                if last_point == self.route_points[-1]:
                    break
            self.processing_complete = True
            await asyncio.shield(self.save_metadata())
        finally:
            if last_save_task:
                await asyncio.shield(last_save_task)
            await asyncio.shield(self.save_panos())


gpx_ns = {
    'gpx11': 'http://www.topografix.com/GPX/1/1',
}


def gpx_get_points(gpx):
    doc = xml.fromstring(gpx)
    trkpts = doc.findall('./gpx11:trk/gpx11:trkseg/gpx11:trkpt', gpx_ns)
    points = [IndexedPoint(lat=float(trkpt.attrib['lat']), lng=float(trkpt.attrib['lon']), index=i)
              for i, trkpt in enumerate(trkpts)]
    return points


def pairs(items):
    return zip(items[:-1], items[1:])


def find_closest_point_pair(points, to_point, req_min_dist=20, stop_after_dist=100):
    tpn = to_point.nv.normal
    min_distance = None
    min_point_pair = None
    min_c_point = None
    for point1, point2 in pairs(points):
        p1 = point1.nv.normal
        p2 = point2.nv.normal
        c12 = cross(p1, p2, axis=0)
        ctp = cross(tpn, c12, axis=0)
        c = unit(cross(ctp, c12, axis=0)).reshape((3, ))

        p1h = p1.reshape((3, ))
        p2h = p2.reshape((3, ))
        dp1p2 = arccos(dot(p1h, p2h))

        sutable_c = None
        for co in (c, 0 - c):
            dp1co = arccos(dot(p1h, co))
            dp2co = arccos(dot(p2h, co))
            if abs(dp1co + dp2co - dp1p2) < 0.000001:
                sutable_c = co
                break

        if sutable_c is not None:
            c_geopoint = wgs84.Nvector(sutable_c.reshape((3, 1))).to_geo_point()
            c_point = Point(lat=c_geopoint.latitude_deg[0], lng=c_geopoint.longitude_deg[0], i_nv_geopoint=c_geopoint)
            distance = to_point.nv_geopoint.distance_and_azimuth(c_geopoint)[0]
        else:
            distance, c_point = min(((to_point.nv_geopoint.distance_and_azimuth(p.nv_geopoint)[0], p) for p in (point1, point2)))

        if min_distance is None or distance < min_distance:
            min_distance = distance
            min_point_pair = (point1, point2)
            min_c_point = c_point

        if min_distance < req_min_dist and distance > stop_after_dist:
            break

    return min_point_pair, min_c_point, min_distance


def iter_points_with_minimal_spacing(points, spacing=10):
    for point1, point2 in pairs(points):
        yield point1
        dist, azi1, azi2 = point1.nv_geopoint.distance_and_azimuth(point2.nv_geopoint)
        pair_points = round(dist / spacing)
        if pair_points:
            pair_spacing = dist / pair_points
            for i in range(1, pair_points - 1):
                point_dist = i * pair_spacing
                add_geopoint = point1.nv_geopoint.geo_point(point_dist, azi1)[0]
                add_point = Point(lat=add_geopoint.latitude_deg, lng=add_geopoint.longitude_deg, i_nv_geopoint=add_geopoint)
                yield add_point
    yield point2


latlng_urlstr = lambda point: "{},{}".format(point.lat, point.lng)


def deg_wrap_to_closest(deg, to_deg):
    up = deg + 360
    down = deg - 360
    return min(deg, up, down, key=lambda x: abs(to_deg - x))


class GoogleApi(object):

    def __init__(self, api_key, cache_db, loop):
        self.session = aiohttp.ClientSession()
        self.api_key = api_key
        self.cache_db = unqlite.UnQLite(cache_db)
        self.loop = loop

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.cache_db.close()

    async def get_pano_ll(self, point, radius=10):
        key = 'll{:=+3.8f}{:=+3.8f}-{}'.format(point.lat, point.lng, radius)
        try:
            id = self.cache_db[key]
        except KeyError:
            async with self.session.get(
                    'http://cbks0.googleapis.com/cbk',
                    params={
                        'output': 'json',
                        'radius': radius,
                        'll': latlng_urlstr(point),
                        'key': self.api_key,
                    }) as r:
                r.raise_for_status()
                text = await r.text()
            data = json.loads(text)
            id = data['Location']['panoId']
            self.cache_db[key] = id
            if id not in self.cache_db:
                self.cache_db[id] = text
            return data
        else:
            return await self.get_pano_id(id)

    @functools.lru_cache()
    async def get_pano_id(self, id):
        try:
            text = self.cache_db[id]
        except KeyError:
            async with self.session.get(
                    'http://cbks0.googleapis.com/cbk',
                    params={
                        'output': 'json',
                        'panoid': id,
                        'key': self.api_key,
                    }) as r:
                r.raise_for_status()
                text = await r.text()
            self.cache_db[id] = text
        return json.loads(text)
