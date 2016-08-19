$(document).ready(function() {
    var split_path_name = window.location.pathname.split('/');
    var path_id = split_path_name[split_path_name.length - 2];

    var sock = new SockJS('/path_sock/'+path_id);
    sock.onopen = function() {
        console.log('open');
    };
    sock.onmessage = function(e) {
        console.log('message', e.data);
    };
    sock.onclose = function() {
        console.log('close');
    };

    sock.send('test');
    sock.close();
});
