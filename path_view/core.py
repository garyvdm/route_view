import os
import logging
import json
import functools
import asyncio
import itertools
import threading
import xml.etree.ElementTree as xml

import aiohttp
import attr
import unqlite
import geographiclib.geodesic
import msgpack
from nvector import (
    unit,
    lat_lon2n_E,
    n_E2lat_lon,
)
from numpy import (
    cross,
    dot,
    arccos,
    deg2rad,
    rad2deg,
)


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
    _nv = attr.ib(default=None, repr=False, cmp=False)

    def to_point(self):
        return self

    @property
    def nv(self):
        if self._nv is None:
            self._nv = lat_lon2n_E(deg2rad(self.lat), deg2rad(self.lng))
        return self._nv


@attr.s(slots=True)
class IndexedPoint(Point):
    index = attr.ib(default=None)
    distance = attr.ib(default=None)

    def to_point(self):
        return Point(self.lat, self.lng)


path_meta_attrs = {'name'}
path_route_attrs = {'route_points', 'route_bounds'}
path_status_attrs = {'processing_at', 'processing_complete', 'processing_status'}


@attr.s
class Path(object):
    id = attr.ib()
    dir_path = attr.ib()
    change_callback = attr.ib()
    name = attr.ib(default=None)
    process_task = attr.ib(default=None, init=False)
    data_loaded = attr.ib(default=False, init=False)
    processing_complete = attr.ib(default=False)
    processing_status = attr.ib(default='')
    route_points = attr.ib(default=None, init=False)
    route_bounds = attr.ib(default=None, init=False)
    panos = attr.ib(default=[], init=False)
    prefered_pano_chain = attr.ib(default={}, init=False)
    processing_at = attr.ib(default=None, init=False)
    panos_len_at_last_save = attr.ib(default=0, init=False)
    save_processing_lock = attr.ib(default=attr.Factory(threading.Lock), init=False)

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
            with open(os.path.join(self.dir_path, 'route.pack'), 'rb') as f:
                route = msgpack.unpack(f, encoding='utf-8')
            route['route_points'] = path_with_distance_and_index(route['route_points'])

            with open(os.path.join(self.dir_path, 'status.json'), 'r') as f:
                status = json.load(f)
            if status['processing_at']:
                status['processing_at']['point'] = Point(*status['processing_at']['point'])

            for k, v in itertools.chain(route.items(), status.items()):
                setattr(self, k, v)

            panos = []
            with open(os.path.join(self.dir_path, 'panos.pack'), 'rb') as f:
                unpacker = msgpack.Unpacker(f, encoding='utf-8')
                while True:
                    try:
                        panos.append(unpacker.unpack())
                    except msgpack.OutOfData:
                        break
            for pano in panos:
                if pano['type'] == 'pano':
                    pano['point'] = Point(*pano['point'])
                if pano['type'] == 'no_images':
                    pano['start_point'] = Point(*pano['start_point'])
                    pano['end_point'] = Point(*pano['end_point'])
            self.panos = panos

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

        with open(os.path.join(self.dir_path, 'route.pack'), 'wb') as f:
            msgpack.pack(self, f, default=json_encode)

    @runs_in_executor
    def clear_saved_panos(self):
        with self.save_processing_lock:
            with open(os.path.join(self.dir_path, 'panos.pack'), 'wb'):
                pass
            self.panos_len_at_last_save = 0

    @runs_in_executor
    def save_processing(self):
        def state_json_encode(obj):
            if isinstance(obj, Point):
                return (obj.lat, obj.lng)
            if isinstance(obj, Path):
                return attr.asdict(obj, recurse=False, filter=lambda a, v: a.name in path_status_attrs)

        def panos_json_encode(obj):
            if isinstance(obj, Point):
                return (obj.lat, obj.lng)

        with self.save_processing_lock:

            with open(os.path.join(self.dir_path, 'status.json'), 'w') as f:
                json.dump(self, f, default=state_json_encode)

            with open(os.path.join(self.dir_path, 'panos.pack'), 'ab') as f:
                for pano in self.panos[self.panos_len_at_last_save:]:
                    msgpack.pack(pano, f, default=panos_json_encode)
            self.panos_len_at_last_save = len(self.panos)

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
        self.route_bounds = dict(
            north=max((p.lat for p in self.route_points)),
            south=min((p.lat for p in self.route_points)),
            east=max((p.lng for p in self.route_points)),
            west=min((p.lng for p in self.route_points)),
        )
        await self.reset_processed()
        self.data_loaded = True
        self.processing_complete = False
        self.change_callback({'route_bounds': self.route_bounds})
        self.change_callback({'route_points': self.route_points, 'route_distance': self.route_points[-1].distance})
        await self.save_route()

    def get_existing_changes(self):
        if self.route_points:
            yield {'status': self.processing_status}
        if self.route_points:
            yield {'route_bounds': self.route_bounds}
            yield {'route_points': self.route_points, 'route_distance': self.route_points[-1].distance}
        if self.panos:
            yield {'panos': self.panos}
        if self.processing_at:
            yield {'processing_at': self.processing_at}

    async def start_processing(self, google_api):
        self.process_task = asyncio.ensure_future(self.process(google_api))
        self.process_task.add_done_callback(self.process_task_done_callback)

    def set_status(self, status):
        self.processing_status = status
        self.change_callback({'status': status})

    def process_task_done_callback(self, fut):
        try:
            fut.result()
        except asyncio.CancelledError:
            logging.info('Processing cancelled.')
            self.set_status('Processing cancelled.')
        except Exception as e:
            logging.exception('Processing error: ')
            self.set_status('Processing error: {}'.format(e))
        self.process_task = None

    async def reset_processed(self):
        self.panos = []
        await self.clear_saved_panos()
        await self.save_processing()
        self.change_callback({'reset': ['panos']})

    async def process(self, google_api):
        self.set_status('Downloading street view image metadata.')

        # if self.panos:
        #     last_pano = self.panos[-1]
        #     last_point_index = self.route_points[last_pano.closest_point_pair_index]
        # else:
        #     last_pano = None
        #     last_point_index = 0
        last_pano = None
        last_point = self.route_points[0]
        last_point_index = 0
        last_at_distance = 0
        last_save_task = None
        last_pano_data = None
        no_pano_link = True
        inverse_line_cached = functools.lru_cache(32)(geodesic.InverseLine)

        new_panos = []
        processing_at = {
            'point': last_point,
            'index': last_point_index,
            'distance': last_at_distance,
            'no_images_from': None
        }

        async def send_changes():
            nonlocal new_panos
            while True:
                await asyncio.sleep(0.2)
                if new_panos or self.processing_at != processing_at:
                    self.panos.extend(new_panos)
                    self.processing_at = processing_at
                    self.change_callback({
                        'panos': new_panos,
                        'processing_at': processing_at
                    })
                    new_panos = []

                if self.processing_complete:
                    break

        send_changes_task = asyncio.ensure_future(send_changes())

        try:
            while True:
                if no_pano_link:
                    points_with_set_spacing = iter_path_points_with_set_spacing(
                        inverse_line_cached, [last_point] + self.route_points[last_point_index + 1:],
                        spacing=itertools.chain(itertools.repeat(10, 3), itertools.repeat(20, 3),
                                                itertools.repeat(50, 10), itertools.repeat(100, 10),
                                                itertools.repeat(200)))
                    if last_point == self.route_points[0]:
                        points_with_set_spacing = itertools.chain(((last_point, last_point, 0, 10), ), points_with_set_spacing)

                    for point, last_path_point, dist_from_last, point_dist in points_with_set_spacing:
                        radius = round(point_dist * 0.75)
                        logging.debug("Get pano at {} radius={}".format(point, radius))
                        pano_data = await google_api.get_pano_ll(point, radius=radius)
                        if pano_data:
                            if last_pano and pano_data['Location']['panoId'] == last_pano['id']:
                                last_point = point
                                last_point_index = (last_path_point.index if isinstance(last_path_point, IndexedPoint) else last_point_index)
                                continue
                            else:
                                no_pano_link = False
                                break
                        else:
                            processing_at = {
                                'point': point,
                                'index': (last_path_point.index if isinstance(last_path_point, IndexedPoint) else last_point_index),
                                'distance': last_at_distance + dist_from_last,
                                'no_images_from': {'point': last_point, 'index': last_point_index, 'distance': last_at_distance},
                            }
                    else:
                        break
                    del points_with_set_spacing
                else:
                    if last_pano['id'] in self.prefered_pano_chain:
                        link_pano_id = self.prefered_pano_chain[last_pano['id']]
                    else:
                        if last_point_index + 2 == len(self.route_points) and distance(last_point, self.route_points[-1]) < 10:
                            break
                        yaw_to_next = get_azimuth_to_distance_on_path(inverse_line_cached, last_point, self.route_points[last_point_index + 1:], 10)
                        yaw_diff = lambda item: abs(deg_wrap_to_closest(float(item['yawDeg']) - yaw_to_next, 0))
                        pano_link = min(last_pano_data['Links'], key=yaw_diff)

                        if yaw_diff(pano_link) > 15:
                            logging.debug("Yaw too different: {} {} {}".format(yaw_diff(pano_link), pano_link['yawDeg'], yaw_to_next))
                            link_pano_id = None
                        else:
                            link_pano_id = pano_link['panoId']

                    if link_pano_id:
                        no_pano_link = False
                        # logging.debug("Getting pano form link: {} -> {}".format(last_pano['id'], link_pano_id))
                        pano_data = await google_api.get_pano_id(link_pano_id)

                        if not pano_data:
                            # What????
                            no_pano_link = True

                    else:
                        no_pano_link = True
                        pano_data = None

                if pano_data:
                    location = pano_data['Location']
                    pano_point = Point(lat=float(location['lat']), lng=float(location['lng']))
                    point_pair, c_point, dist = find_closest_point_pair(self.route_points[last_point_index:], pano_point)

                    if dist > 15:
                        logging.debug("Distance {} to nearest point too great for pano: {}"
                                      .format(dist, location['panoId']))
                        last_pano = None
                        no_pano_link = True
                    else:
                        heading = get_azimuth_to_distance_on_path(inverse_line_cached, c_point, self.route_points[point_pair[1].index:], 50)
                        distance_from_last = distance(point_pair[0], c_point)
                        c_point_dist = point_pair[0].distance + distance(point_pair[0], c_point)

                        if c_point_dist - last_at_distance > 100:
                            new_panos.append(dict(
                                type='no_images',
                                start_point=last_point.to_point(), start_index=last_point_index + 1,
                                end_point=c_point.to_point(), end_index=point_pair[0].index,
                                start_distance=last_at_distance, end_distance=c_point_dist - 10,
                            ))
                            distance_from_last = 10

                        pano = dict(
                            type='pano', id=location['panoId'], point=pano_point,
                            description=location['description'], i=last_point_index, heading=heading,
                            at_dist=c_point_dist, dist_from_last=distance_from_last)
                        new_panos.append(pano)

                        # logging.debug("Got pano {} {}".format(pano_point, location['description']))
                        last_pano = pano
                        last_pano_data = pano_data
                        last_point_index = point_pair[0].index
                        last_point = c_point
                        last_at_distance = c_point_dist

                        processing_at = {
                            'point': last_point,
                            'index': last_point_index,
                            'distance': last_at_distance,
                            'no_images_from': None,
                        }

                        if (not last_save_task or last_save_task.done()) and len(self.panos) - self.panos_len_at_last_save > 100:
                            if last_save_task:
                                await asyncio.shield(last_save_task)
                            last_save_task = asyncio.ensure_future(self.save_processing())

                if last_point == self.route_points[-1]:
                    break

            if self.route_points[-1].distance - last_at_distance > 100:
                new_panos.append(dict(
                    type='no_images',
                    start_point=last_point.to_point(), start_index=last_point_index + 1,
                    end_point=self.route_points[-1].to_point(), end_index=len(self.route_points) - 2,
                    start_distance=last_pano['at_distance'] + 1, end_distance=self.route_points[-1].distance,
                ))

            self.processing_complete = True
            processing_at = {
                'point': self.route_points[-1],
                'index': self.route_points[-1].index,
                'distance': self.route_points[-1].distance,
                'no_images_from': None,
            }
            await send_changes_task
            self.set_status('Complete')
        finally:
            if last_save_task:
                await asyncio.shield(last_save_task)
            await asyncio.shield(self.save_processing())


gpx_ns = {
    'gpx11': 'http://www.topografix.com/GPX/1/1',
}


def gpx_get_points(gpx):
    doc = xml.fromstring(gpx)
    trkpts = doc.findall('./gpx11:trk/gpx11:trkseg/gpx11:trkpt', gpx_ns)
    points = path_with_distance_and_index((float(trkpt.attrib['lat']), float(trkpt.attrib['lon'])) for trkpt in trkpts)
    return points


def path_with_distance_and_index(path):
    dist = 0
    previous_point = None

    def get_point(i, point):
        nonlocal dist
        nonlocal previous_point
        point = IndexedPoint(*point, index=i)
        if previous_point:
            dist += distance(previous_point, point)
        point.distance = dist
        previous_point = point
        return point

    return [get_point(i, point) for i, point in enumerate(path)]


def pairs(items):
    itr = iter(items)
    item1 = next(itr)
    for item2 in itr:
        yield item1, item2
        item1 = item2


def find_closest_point_pair(points, to_point, req_min_dist=20, stop_after_dist=50):
    tpn = to_point.nv
    min_distance = None
    min_point_pair = None
    min_c_point = None
    for point1, point2 in pairs(points):
        p1 = point1.nv
        p2 = point2.nv
        c12 = cross(p1, p2, axis=0)
        ctp = cross(tpn, c12, axis=0)
        c = unit(cross(ctp, c12, axis=0))
        p1h = p1.reshape((3, ))
        p2h = p2.reshape((3, ))
        dp1p2 = arccos(dot(p1h, p2h))

        sutable_c = None
        for co in (c, 0 - c):
            co_rs = co.reshape((3, ))
            dp1co = arccos(dot(p1h, co_rs))
            dp2co = arccos(dot(p2h, co_rs))
            if abs(dp1co + dp2co - dp1p2) < 0.000001:
                sutable_c = co
                break

        if sutable_c is not None:
            c_point_lat, c_point_lng = n_E2lat_lon(sutable_c)
            c_point = Point(lat=rad2deg(c_point_lat[0]), lng=rad2deg(c_point_lng[0]))
            c_dist = distance(to_point, c_point)
        else:
            c_dist, c_point = min(((distance(to_point, p), p) for p in (point1, point2)))

        if min_distance is None or c_dist < min_distance:
            min_distance = c_dist
            min_point_pair = (point1, point2)
            min_c_point = c_point

        if min_distance < req_min_dist and c_dist > stop_after_dist:
            break

    return min_point_pair, min_c_point, min_distance


def iter_path_points_with_set_spacing(inverse_line_cached, points, spacing=10):
    distance_covered = 0
    try:
        spacing = iter(spacing)
    except TypeError:
        spacing = itertools.repeat(spacing)
    next_spacing = next(spacing)

    prev_point_remaining = 0

    for point1, point2 in pairs(points):
        pair_geo_line = inverse_line_cached(point1.lat, point1.lng, point2.lat, point2.lng)
        pair_distance_covered = 0 - prev_point_remaining
        while next_spacing + pair_distance_covered < pair_geo_line.s13:
            pair_distance_covered += next_spacing
            geo = pair_geo_line.Position(pair_distance_covered)
            yield Point(lat=geo['lat2'], lng=geo['lon2']), point1, distance_covered + pair_distance_covered, next_spacing
            next_spacing = next(spacing)
        distance_covered += pair_geo_line.s13
        prev_point_remaining = pair_geo_line.s13 - pair_distance_covered


geodesic = geographiclib.geodesic.Geodesic.WGS84


def distance(point1, point2):
    return geodesic.Inverse(point1.lat, point1.lng, point2.lat, point2.lng)['s12']


def distance_and_azimuth(point1, point2):
    geo = geodesic.Inverse(point1.lat, point1.lng, point2.lat, point2.lng)
    return geo['s12'], geo['azi1']


def point_from_distance_and_azimuth(point, distance, azimuth):
    geo = geodesic.Direct(point.lat, point.lng, azimuth, distance)
    return Point(lat=geo['lat2'], lng=geo['lon2'])


def geo_from_distance_on_path(inverse_line_cached, path, dist):
    distance_covered = 0
    first_pair = True
    for point1, point2, in pairs(path):
        pair_geo_line = inverse_line_cached(point1.lat, point1.lng, point2.lat, point2.lng)
        if distance_covered + pair_geo_line.s13 < dist:
            distance_covered += pair_geo_line.s13
            first_pair = False
        else:
            geo = pair_geo_line.Position(dist - distance_covered)
            if not first_pair:
                geo = geodesic.Inverse(path[0].lat, path[0].lng, geo['lat2'], geo['lon2'])
            return geo
    return geodesic.Inverse(path[0].lat, path[0].lng, path[-1].lat, path[-1].lng)


def get_azimuth_to_distance_on_path(inverse_line_cached, from_point, path, dist):
    to_geo = geo_from_distance_on_path(inverse_line_cached, [from_point] + path, dist)
    return to_geo['azi1']


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

    async def get_pano_ll(self, point, radius=15):
        key = 'll{:=+3.8f}{:=+3.8f}-{}'.format(point.lat, point.lng, radius)
        try:
            id = self.cache_db[key]
        except KeyError:
            async with self.session.get(
                    'http://cbks0.googleapis.com/cbk',
                    params={
                        'output': 'json',
                        'radius': round(radius),
                        'll': latlng_urlstr(point),
                        'key': self.api_key,
                    }) as r:
                r.raise_for_status()
                text = await r.text()
            try:
                data = json.loads(text)
            except json.decoder.JSONDecodeError as e:
                logging.error('Bad JSON from api: {}\n {}'.format(e, text))
                raise
            if data:
                id = data['Location']['panoId']
                self.cache_db[key] = id
                if id not in self.cache_db:
                    self.cache_db[id] = text
            return data
        else:
            return (await self.get_pano_id(id))

    async def get_pano_id(self, id):
        try:
            text = self.cache_db[id]
            # If the whole route is cached, we may end up blocking for a long time. quick sleep so we don't
            await asyncio.sleep(0)
            from_cache = True
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
            from_cache = False
        try:
            data = json.loads(text)
        except json.decoder.JSONDecodeError as e:
            logging.error('Bad JSON from api: {}\n {}'.format(e, text))
            if from_cache:
                del self.cache_db[id]
            raise
        if not from_cache:
            self.cache_db[id] = text
        return data
