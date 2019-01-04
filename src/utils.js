const util = require("util");
const _ = require("lodash");
const fp = require("lodash/fp");

function output(s) {
    process.stdout.write(s + "\n");
}

function debug(s) {
    process.stderr.write(s + "\n");
}

function debugInspect(obj) {
    debug(inspect(obj));
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

function get(obj, path, {defaultValue} = {}) {
    const value = _.get(obj, path);

    if (_.isUndefined(value)) {
        if (_.isUndefined(defaultValue)) {
            throw new Error(`No path in obj: ${inspect(obj)} -> ${path.join(", ")}`);
        } else {
            return defaultValue;
        }
    } else {
        return value;
    }
}

function interpolate(template, namespace) {
    const names = Object.keys(namespace);
    const values = Object.values(namespace);
    try {
        return new Function(...names, `return \`${template}\`;`)(...values);
    } catch (err) {
        console.error(`Interpolate error: template=${template}`); // eslint-disable-line no-console
        throw err;
    }
}

function cartesianProduct(...rest) {
    return fp.reduce((a, b) =>
        fp.flatMap(x =>
            fp.map(y => x.concat([y]))(b)
        )(a)
    )([[]])(rest);
}

module.exports = {
    output,
    debug,
    debugInspect,
    repeat,
    base64Encode,
    inspect,
    interpolate,
    get,
    cartesianProduct,
};
