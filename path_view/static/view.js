$(document).ready(function() {
    var map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: 0, lng: 0},
        zoom: 2
    });

    var route_polyline = new google.maps.Polyline({
        path: [],
        geodesic: false,
        strokeColor: '#0000FF',
        strokeOpacity: 1.0,
        strokeWeight: 2,
        map: map
    });
    var pano_polyline = new google.maps.Polyline({
        path: [],
        geodesic: false,
        strokeColor: '#FF0000',
        strokeOpacity: 1.0,
        strokeWeight: 2,
        map: map
    });
    var position_marker = new google.maps.Marker({
        map: map,
    });

    var pano_display = document.getElementById('pano_display');

    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];
    var ws = new WebSocket('ws://' + location.host + '/path_sock/' + path_id + '/');
    var panos = []

    ws.onmessage = function(e) {
        var data = $.parseJSON(e.data);
        if (data.hasOwnProperty('route_bounds')) {
            map.fitBounds(data['route_bounds'])
        }
        if (data.hasOwnProperty('route_points')) {
            route_polyline.setPath(data.route_points)
        }
        if (data.hasOwnProperty('panos')) {
            panos = panos.concat(data.panos);
            load_next_panos();
            new_path = panos.map(function(pano){return pano.point});
            if (data.panos < 5) {
                pano_polyline.path.insertAt(pano_polyline.path.length, data.panos)
            } else {
                pano_polyline.setPath(new_path);
            }
        }
    };

    var num_panos_loading = 0;
    var max_panos_loading = 8;
    var panos_loaded_to = -1;
    function load_next_panos(){
        while (num_panos_loading < max_panos_loading && panos_loaded_to < panos.length - 1) {
            panos_loaded_to ++;
            load_pano(panos[panos_loaded_to]);
        }
    };

    function load_pano(pano){

        pano.image = new Image()
        pano.image.src = 'https://maps.googleapis.com/maps/api/streetview?size=640x480&pano=' + pano.id + '&heading=' + pano.heading + '&sensor=false&fov=110'
        num_panos_loading++;
        pano.image.onload = function(){
            num_panos_loading--;
            load_next_panos();
            pano.img_src = pano.image.src;
            delete pano.img;
            if (!show_next_pano_timeout) show_next_pano();
        };
    }

    var current_pano = -1;
    var show_next_pano_timeout = null;

    function show_next_pano(){
        var pano = panos[current_pano + 1];
        if (!pano.hasOwnProperty('image') && !pano.hasOwnProperty('img_src')) {
            load_pano(pano);
        }
        if (!pano.hasOwnProperty('img_src')) {
            show_next_pano_timeout = null
        } else {
            current_pano++;
            pano_display.src = pano.img_src;
            position_marker.setPosition(pano.point);
            if (current_pano < panos.length -1 ) {
                show_next_pano_timeout = setTimeout(show_next_pano, 100);
            }
        }
    };

});
