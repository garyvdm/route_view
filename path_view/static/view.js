document.addEventListener('DOMContentLoaded', function() {

    var pano_rotate = new google.maps.StreetViewPanorama(document.getElementById('pano_rotate'),{
        imageDateControl: true,
        visible: false
    });

    var map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: 0, lng: 0},
        zoom: 2,
        streetView: pano_rotate,
        mapTypeId: 'terrain'
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
        strokeColor: '#0000imageDateControlFF',
        strokeOpacity: 1.0,
        strokeWeight: 2,
        map: map
    });
    var position_marker = new google.maps.Marker({
        map: map,
    });
    var no_images_polyline = {};

    var pano_play = document.getElementById('pano_play');
    var pano_rotate_contain = document.getElementById('pano_rotate_contain');
    var no_images = document.getElementById('no_images');

    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];
    var ws = new WebSocket('ws://' + location.host + '/path_sock/' + path_id + '/');
    var panos = [];
    var api_key = '';
    var route_points = [];
    var total_distance = null;

    var processing_status = document.getElementById('processing_status');
    var play_status = document.getElementById('play_status');
    var play_pause = document.getElementById('play_pause');
    var play_pause_img = play_pause.querySelector('img')
    var seek = document.getElementById('seek');

    var processing_progress = document.getElementById("processing_progress").getContext("2d");
    var buffer_progress = document.getElementById("buffer_progress").getContext("2d");
    var play_progress = document.getElementById("play_progress")
    var play_progress_context = play_progress.getContext("2d");
    var cancel = document.getElementById('cancel');
    var resume = document.getElementById('resume');
    var show_pano_markers = document.getElementById('show_pano_markers');

    processing_progress.fillStyle = "#8080FF";
    processing_progress.fillRect(0, 0, 1000, 10);

    ws.onmessage = function(e) {
        var data = JSON.parse(e.data);
//        console.log(data);
        if (data.hasOwnProperty('status')) {
            processing_status.textContent = data.status.text;
            cancel.style.display = data.status.cancelable ? '' : 'none';
            resume.style.display = data.status.resumable ? '' : 'none';
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
            panos = panos.concat(data.panos);
            load_next_panos();
            continue_play();

            var no_images_by_start_point = data.panos.reduce(function (memo, item) {
                if (item.type == 'no_images'){
                    memo[item.start_point.lat+','+item.start_point.lat] = item
                }
                return memo;
            }, {})

            var no_images;
            for (var key in no_images_by_start_point) {
                if (!no_images_by_start_point.hasOwnProperty(key)) continue;
                if (no_images_polyline.hasOwnProperty(key)) {
                    no_images_polyline[key].setMap(null)
                }
                no_images = no_images_by_start_point[key];
                path = [no_images.start_point].concat(
                    route_points.slice(no_images.start_route_index, no_images.prev_route_index),
                    [no_images.point]);
                polyline = new google.maps.Polyline({
                    path: path,
                    geodesic: false,
                    strokeColor: '#FF0000',
                    strokeOpacity: 1.0,
                    strokeWeight: 2.5,
                    map: map,
                    zIndex: 1
                });
                no_images_polyline[key] = polyline
                buffer_progress.fillStyle = "#FF0000";
                buffer_progress.fillRect(
                    1000 * no_images.at_dist / total_distance, 0,
                    1000 * (0 - no_images.start_dist_from) / total_distance, 10
                );
            }

            var last_pano = panos[panos.length - 1]
            var processed_path = route_points.slice(0, last_pano.prev_route_index).concat([last_pano.point]);
            processed_polyline.setPath(processed_path);
            processing_progress.fillStyle = "#4040FF";
            processing_progress.fillRect(0, 0, 1000 * last_pano.at_dist / total_distance, 10);
        }

    };

    cancel.addEventListener('click', function (){
        ws.send(JSON.stringify('cancel'));
    });

    resume.addEventListener('click', function (){
        ws.send(JSON.stringify('resume'));
    });

    var playing = true;

    function pause() {
        playing = false;
        if (show_next_pano_timeout) {
            clearTimeout(show_next_pano_timeout);
            show_next_pano_timeout = null;
        }
        play_pause_img.src = '/static/play.png';
    }

    function play() {
        playing = true;
        pano_rotate.setVisible(false);
        continue_play()
        play_pause_img.src = '/static/pause.png';
    }

    function continue_play() {
        var pano_index = current_pano_index + 1
        if (playing && !show_next_pano_timeout && !show_delayed && pano_index < panos.length ) {
            show_next_pano_timeout = setTimeout(show_pano, 100, pano_index);
        }
    }

    play_pause.addEventListener('click', function (){
        if (playing) {
            pause();
            show_pano_rotate(current_pano_index);
        } else {
            play();
        }
    });

    function show_pano_rotate(pano_index){
        var pano = panos[pano_index];
        pano_rotate.setPano(pano.id);
        pano_rotate.setPov({'heading': pano.heading, 'pitch': 0});
        pano_rotate.setZoom(1);
        // hide off screen, so that we can show when images have loaded
        pano_rotate_contain.style.left = '-10000px';
        pano_rotate.setVisible(true);
    }

    pano_rotate.addListener('visible_changed', function() {
        if (pano_rotate.getVisible()){
            pause();
            var status_changed_list = pano_rotate.addListener('status_changed', function() {
                setTimeout(function (){
                    pano_rotate_contain.style.left = '0';
                }, 500);
                google.maps.event.removeListener(status_changed_list);
            });
        }
    });

    var num_panos_loading = 0;
    var max_panos_loading = 8;
    var panos_loaded_at = -1;
    var current_pano_index = -1;

    function load_next_panos(){
        var processed_this_func = 0;
        while (num_panos_loading < max_panos_loading && panos_loaded_at < panos.length - 1 && panos_loaded_at < current_pano_index + 500 ) {
            panos_loaded_at ++;
            load_pano(panos_loaded_at);
            processed_this_func ++;
            if (processed_this_func >= 50){
                setTimeout(load_next_panos, 100);
                break;
            }
        }
    };

    function load_pano(pano_index) {
        var pano = panos[pano_index];
        if (pano.type == 'pano'){
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
                    1000 * (0 - pano.dist_from_last) / total_distance, 10
                );

                if (pano_index == current_pano_index && show_delayed) show_pano(pano_index);
            };
        }
    }

    var show_next_pano_timeout = null;

    var show_delayed = false;

    function show_pano(pano_index){
        if (show_next_pano_timeout) {
            clearTimeout(show_next_pano_timeout);
            show_next_pano_timeout = null;
        }
        current_pano_index = pano_index
        var pano = panos[pano_index];

        play_progress_context.clearRect(0, 0, 1000, 10);
        play_progress_context.fillStyle = "#FFFFFF";
        play_progress_context.fillRect(1000 * pano.at_dist / total_distance, 0, -2, 10);

        show_delayed = false;

        if (pano.type == 'pano') {
            if (!pano.hasOwnProperty('image') && !pano.hasOwnProperty('img_src')) {
                load_pano(pano_index);
            }
            if (pano.hasOwnProperty('img_src')) {
                no_images.style.display = 'none';
                pano_play.src = pano.img_src;
            } else {
                show_delayed = true;
            }
        }

        if (pano.type == 'no_images'){
            no_images.style.display = '';
        }

        if (!show_delayed) {
            if (!map.getBounds().contains(pano.point)) map.panTo(pano.point);
            position_marker.setPosition(pano.point);
            continue_play();
            load_next_panos();
        }
    }

    play_progress.addEventListener('click', function(e){
        var rect = play_progress.getBoundingClientRect();
        var seek_distance  = (e.offsetX || e.pageX - rect.left + document.body.scrollLeft) / play_progress.offsetWidth * total_distance;
        function get_pano_at_dist(pano) {
            return pano.at_dist;
        }
        var pano_index = binarySearchClosest(panos, seek_distance, get_pano_at_dist);
        panos_loaded_at = pano_index - 1;
        if (pano_index > -1) {
            pano_play.src = '';
            show_pano(pano_index);
            if (!playing) {
                show_pano_rotate(pano_index);
            }
        }
    });


    var pano_markers = [];
    function do_show_pano_markers(){
        pano_markers.forEach(function(marker){ marker.setMap(null); });
        pano_markers = [];
        if (show_pano_markers.checked && map.getZoom() > 17) {
            var bounds = map.getBounds();
            panos.forEach(function(pano){
                if (pano.type == 'pano' && bounds.contains(pano.original_point)) {
                    var marker = new google.maps.Marker({
                        position: pano.original_point,
                        map: map,
                        icon: {
                          path: google.maps.SymbolPath.CIRCLE,
                          scale: 1
                        }
                    })
                    google.maps.event.addListener(marker, 'click', do_show_pano_markers);
                    pano_markers.push(marker);
                }
            });
        }
    }
    show_pano_markers.addEventListener('change', do_show_pano_markers);
    google.maps.event.addListener(map, 'idle', do_show_pano_markers);

}, false);


function binarySearchClosest(arr, search, key) {

    var arrLength = arr.length;
    var minIndex = 0;
    var maxIndex = arrLength - 2;
    var currentIndex;
    var currentElement, currentKey;
    var nextElement, nextKey;


    if (key === undefined) {
        key = function(item) { return item; }
    }

    while (minIndex <= maxIndex) {
        currentIndex = (minIndex + maxIndex) / 2 | 0;
        currentElement = arr[currentIndex];
        currentKey = key(currentElement)
        nextElement = arr[currentIndex + 1]
        nextKey = key(nextElement)

        if ((currentKey < search  && search < nextKey) || maxIndex == 0 || minIndex == arrLength - 2) {
            return currentIndex;
        } else if (nextKey > search) {
            maxIndex = currentIndex;
        }
        else if (currentKey < search) {
            minIndex = currentIndex + 1;
        }
    }

    return -1;
}
