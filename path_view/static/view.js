$(document).ready(function() {
    var map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: 0, lng: 0},
        zoom: 2
    });

    var route_polyline = new google.maps.Polyline({
        path: [],
        geodesic: false,
        strokeColor: '#0000FF',
        strokeOpacity: 0.5,
        strokeWeight: 1.8,
        map: map
    });
    var processed_polyline = new google.maps.Polyline({
        path: [],
        geodesic: false,
        strokeColor: '#0000FF',
        strokeOpacity: 1.0,
        strokeWeight: 2,
        map: map
    });
    var position_marker = new google.maps.Marker({
        map: map,
    });
    var processed_to_no_images = new google.maps.Polyline({
        path: [],
        geodesic: false,
        strokeColor: '#FF0000',
        strokeOpacity: 1.0,
        strokeWeight: 2.5,
        map: map,
        visible: false
    });


    var no_images_polyline = [];

    var pano_display = document.getElementById('pano_display');

    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];
    var ws = new WebSocket('ws://' + location.host + '/path_sock/' + path_id + '/');
    var panos = []
    var api_key = ''
    var route_points = []

    ws.onmessage = function(e) {
        var data = $.parseJSON(e.data);
//        console.log(data);
        if (data.hasOwnProperty('api_key')) {
            api_key = '&key=' + data.api_key
        }
        if (data.hasOwnProperty('route_bounds')) {
            map.fitBounds(data['route_bounds'])
        }
        if (data.hasOwnProperty('route_points')) {
            route_points = data.route_points
            route_polyline.setPath(data.route_points)
        }
        if (data.hasOwnProperty('panos')) {
            var new_panos = data.panos.filter(function (pano) {return pano.type == 'pano'});
            if (new_panos.length > 0){
                panos = panos.concat(new_panos);
                load_next_panos();
                if (!show_next_pano_timeout) show_next_pano();
            }
            var new_no_images = data.panos.filter(function (pano) {return pano.type == 'no_images'});

            var i, no_images, path, new_no_images_len = new_no_images.length;
            for (i=0; i<new_no_images_len; i++) {
                no_images = new_no_images[i];
                path = [no_images.start_point].concat(
                    route_points.slice(no_images.start_index, no_images.end_index + 1),
                    [no_images.end_point]);
                console.log(path)

                no_images_polyline.push(new google.maps.Polyline({
                    path: path,
                    geodesic: false,
                    strokeColor: '#FF0000',
                    strokeOpacity: 1.0,
                    strokeWeight: 2.5,
                    map: map
                }));
            }
        }
        if (data.hasOwnProperty('processing_at')) {
            var path = route_points.slice(0, data.processing_at.index + 1).concat([data.processing_at.point]);
            processed_polyline.setPath(path);

            var no_images_from = data.processing_at.no_images_from;
            if (no_images_from){
                path = [no_images_from.point].concat(
                    route_points.slice(no_images_from.index, data.processing_at.index + 1),
                    [data.processing_at.point]);
                processed_to_no_images.setPath(path);
                processed_to_no_images.setVisible(true);
            } else {
                processed_to_no_images.setVisible(false);
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
        pano.image.src = 'https://maps.googleapis.com/maps/api/streetview?size=640x480&pano=' + pano.id + '&heading=' + pano.heading + '&sensor=false&fov=110' + api_key
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
        show_next_pano_timeout = null
        if (current_pano + 1 < panos.length){
            var pano = panos[current_pano + 1];
            if (!pano.hasOwnProperty('image') && !pano.hasOwnProperty('img_src')) {
                load_pano(pano);
            }
            if (!pano.hasOwnProperty('img_src')) {
            } else {
                current_pano++;
                pano_display.src = pano.img_src;
                position_marker.setPosition(pano.point);
                if (current_pano < panos.length -1 ) {
                    show_next_pano_timeout = setTimeout(show_next_pano, 100);
                }
            }
        }
    };

});
