$(document).ready(function() {
    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];
    var ws = new WebSocket('ws://' + location.host + '/path_sock/' + path_id + '/');

    ws.onmessage = function(e) {
        console.log('ws message', data);

        var data = $.parseJSON(e.data);
        if (data.hasOwnProperty('panos')) {
            var i, pano, panos_len, panos = data.panos, panos_len = panos.length;
            var $panos = $('#panos');
            for (i = 0; i < panos_len; i++) {
                pano = panos[i]
                $panos.append($('<img src="https://maps.googleapis.com/maps/api/streetview?size=640x480&pano=' + pano.id + '&heading=' + pano.heading + '&sensor=false&fov=110" style="display: block;">'))

            }
        } else {
            console.log('ws message', data);
        }
    };

});
