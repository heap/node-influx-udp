var dgram = require('dgram');
var _ = require('lodash');

function InfluxUdp(opts) {
    opts = opts || {};
    this.host = opts.host || '127.0.0.1';
    this.port = opts.port || 4444;
    this.socketType = opts.socketType || 'udp4'
}

function convertObjectToKV(obj) {
    return Object.keys(obj).map(function(key) {
        return key + '=' + obj[key]
    }).join();
}

function convertPointToLine(seriesName, point) {
    result = [
        seriesName,
        point.tags ? ',' : '',
        convertObjectToKV(point.tags || {}),
        ' ',
        convertObjectToKV(point.values)
    ].join('');

    return result;
}

function lineProtocol(points) {
    // points : { some-series: [ { values: { value: ... }, tags: {...} } ]
    return new Buffer(
        _.reduce(points, function(ret, value, key) {
            return ret.concat(value.map(_.partial(convertPointToLine, key)));
        }, []).join('\n')
    );
}

InfluxUdp.prototype.send = function influxSend(points) {
    var message = lineProtocol(points);

    socket = dgram.createSocket(this.socketType);
    socket.send(message, 0, message.length, this.port, this.host, function () {
        socket.close();
    });
};

InfluxUdp.prototype.writePoints = function(seriesName, points, options, callback) {
    var data = {};
    data[seriesName] = points.map(function(point) {
        return {
            values: { value: point[0] },
            tags: point[1]
        }
    });
    this.send(data);
    callback();
};

module.exports = InfluxUdp;
