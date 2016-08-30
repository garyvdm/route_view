$(document).ready(function() {
    var map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: 0, lng: 0},
        zoom: 2
    });


    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];
    var ws = new WebSocket('ws://' + location.host + '/path_sock/' + path_id + '/');

    ws.onmessage = function(e) {
        var data = $.parseJSON(e.data);
        console.log('ws message', data);
        if (data.hasOwnProperty('route_bounds')) {
            map.fitBounds(data['route_bounds'])
        }
        if (data.hasOwnProperty('route_points')) {
            new google.maps.Polyline({
                path: data.route_points,
                geodesic: true,
                strokeColor: '#0000FF',
                strokeOpacity: 1.0,
                strokeWeight: 2
            }).setMap(map);
        }
        if (data.hasOwnProperty('panos')) {
            var i, pano, panos_len, panos = data.panos, panos_len = panos.length;
            var $panos = $('#panos');
            for (i = 0; i < panos_len; i++) {
                pano = panos[i]
                $panos.append($('<img src="https://maps.googleapis.com/maps/api/streetview?size=640x480&pano=' + pano.id + '&heading=' + pano.heading + '&sensor=false&fov=110" style="display: block;">'))

            }
        }
    };

});
