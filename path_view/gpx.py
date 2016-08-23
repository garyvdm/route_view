import xml.etree.ElementTree as xml

from path_view.point import Point

ns = {
    'gpx11': 'http://www.topografix.com/GPX/1/1',
}


def get_points(gpx):
    doc = xml.fromstring(gpx)
    trkpts = doc.findall('./gpx11:trk/gpx11:trkseg/gpx11:trkpt', ns)
    points = [Point(trkpt.attrib['lat'], trkpt.attrib['lon']) for trkpt in trkpts]
    return points
