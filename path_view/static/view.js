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
    var panos = [];
    var api_key = '';
    var route_points = [];
    var total_distance = null;

    var $processing_status = $('#processing_status');
    var $play_status = $('#play_status');
    var $play_pause = $('#play_pause');
    var $seek = $('#seek');

    var processing_progress = document.getElementById("processing_progress").getContext("2d");
    var buffer_progress = document.getElementById("buffer_progress").getContext("2d");
    var play_progress = document.getElementById("play_progress").getContext("2d");


    processing_progress.fillStyle = "#8080FF";
    processing_progress.fillRect(0, 0, 1000, 10);

    ws.onmessage = function(e) {
        var data = $.parseJSON(e.data);
//        console.log(data);
        if (data.hasOwnProperty('status')) {
            $processing_status.text(data.status);
        }
        if (data.hasOwnProperty('api_key')) {
            api_key = '&key=' + data.api_key;
        }
        if (data.hasOwnProperty('route_bounds')) {
            map.fitBounds(data['route_bounds']);
        }
        if (data.hasOwnProperty('route_points')) {
            route_points = data.route_points;
            route_polyline.setPath(data.route_points);
            total_distance = data.route_distance;
        }
        if (data.hasOwnProperty('panos')) {
            var new_panos = data.panos.filter(function (pano) {return pano.type == 'pano'});
            if (new_panos.length > 0){
                panos = panos.concat(new_panos);
                load_next_panos();
                if (!show_next_pano_timeout && playing) show_next_pano();
            }
            var new_no_images = data.panos.filter(function (pano) {return pano.type == 'no_images'});

            var i, no_images, path, new_no_images_len = new_no_images.length;
            for (i=0; i<new_no_images_len; i++) {
                no_images = new_no_images[i];
                path = [no_images.start_point].concat(
                    route_points.slice(no_images.start_index, no_images.end_index + 1),
                    [no_images.end_point]);

                no_images_polyline.push(new google.maps.Polyline({
                    path: path,
                    geodesic: false,
                    strokeColor: '#FF0000',
                    strokeOpacity: 1.0,
                    strokeWeight: 2.5,
                    map: map
                }));
                buffer_progress.fillStyle = "#FF0000";
                buffer_progress.fillRect(
                    1000 * no_images.start_distance / total_distance, 0,
                    1000 * (no_images.end_distance - no_images.start_distance) / total_distance, 10
                );

            }
        }
        if (data.hasOwnProperty('processing_at')) {
            var path = route_points.slice(0, data.processing_at.index + 1).concat([data.processing_at.point]);
            processed_polyline.setPath(path);
            processing_progress.fillStyle = "#4040FF";
            processing_progress.fillRect(0, 0, 1000 * data.processing_at.distance / total_distance, 10);

            var no_images_from = data.processing_at.no_images_from;
            if (no_images_from){
                path = [no_images_from.point].concat(
                    route_points.slice(no_images_from.index, data.processing_at.index + 1),
                    [data.processing_at.point]);
                processed_to_no_images.setPath(path);
                processed_to_no_images.setVisible(true);
                processing_progress.fillStyle = "#FF0000";
                processing_progress.fillRect(
                    1000 * no_images_from.distance / total_distance, 0,
                    1000 * (data.processing_at.distance - no_images_from.distance) / total_distance, 10
                );
            } else {
                processed_to_no_images.setVisible(false);
            }
        }

    };

    var playing = true;
    $play_pause.click(function (){
        if (playing) {
            playing = false;
            if (show_next_pano_timeout) {
                clearTimeout(show_next_pano_timeout);
                show_next_pano_timeout = null;
            }
            $play_pause.find('img').attr('src', '/static/play.png');
        } else {
            playing = true;
            if (!show_next_pano_timeout) show_next_pano();
            $play_pause.find('img').attr('src', '/static/pause.png');
        }
    });
    $play_pause.prop( "disabled", false );

    var num_panos_loading = 0;
    var max_panos_loading = 8;
    var panos_loaded_at = -1;
    var current_pano = -1;

    function load_next_panos(){
        while (num_panos_loading < max_panos_loading && panos_loaded_at < panos.length - 1 && panos_loaded_at < current_pano + 100 ) {
            panos_loaded_at ++;
            load_pano(panos[panos_loaded_at]);
        }
    };

    function load_pano(pano){

        pano.image = new Image();
        pano.image.src = 'https://maps.googleapis.com/maps/api/streetview?size=640x480&pano=' + pano.id + '&heading=' + pano.heading + '&sensor=false&fov=110' + api_key
        num_panos_loading++;
        pano.image.onload = function(){
            num_panos_loading--;
            load_next_panos();
            pano.img_src = pano.image.src;
            delete pano.img;
            buffer_progress.fillStyle = "#0000FF";
            buffer_progress.fillRect(
                1000 * pano.at_dist / total_distance, 0,
                1000 * (0 - pano.dist_from_last + 5) / total_distance, 10
            );

            if (!show_next_pano_timeout && playing) show_next_pano();
        };
    }

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
                play_progress.clearRect(0, 0, 1000, 10);
                play_progress.fillStyle = "#FFFFFF";
                play_progress.fillRect(1000 * pano.at_dist / total_distance, 0, -2, 10);
                pano_display.src = pano.img_src;
                position_marker.setPosition(pano.point);
                if (current_pano < panos.length -1 ) {
                    show_next_pano_timeout = setTimeout(show_next_pano, 100);
                }
                load_next_panos();
            }
        }
    };

});
