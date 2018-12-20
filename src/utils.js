const util = require("util");
const _ = require("lodash");

function output(s) {
    process.stdout.write(s + "\n");
}

function debug(s) {
    process.stderr.write(s + "\n");
}

function repeat(value, n) {
    return _.flatten(_.times(n, _.constant([value])));
}

function base64Encode(s) {
    return Buffer.from(s).toString("base64");
}

function inspect(obj) {
    return util.inspect(obj, false, null, true);
}

function interpolate(template, namespace) {
    const names = Object.keys(namespace);
    const values = Object.values(namespace);
    return new Function(...names, `return \`${template}\`;`)(...values);
}

module.exports = {output, debug, repeat, base64Encode, inspect, interpolate};
