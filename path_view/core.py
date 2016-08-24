import os
import xml.etree.ElementTree as xml

import attr


@attr.s(slots=True)
class Point(object):
    lat = attr.ib()
    lng = attr.ib()


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
