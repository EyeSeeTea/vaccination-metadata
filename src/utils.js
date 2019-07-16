const util = require("util");
const fs = require("fs");
const os = require("os");
const _ = require("lodash");
const fp = require("lodash/fp");

const exec = util.promisify(require("child_process").exec);

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

function getOrThrow(obj, path, { defaultValue } = {}) {
    const value = _.get(obj, path);

    if (_.isUndefined(value)) {
        if (_.isUndefined(defaultValue)) {
            const pathString = _(path)
                .castArray()
                .join(" -> ");
            throw new Error(`No path '${pathString}' in object:\n${inspect(Object.keys(obj))}`);
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
        console.error(`Interpolate error.\nTemplate: ${template}`); // eslint-disable-line no-console
        throw err;
    }
}

function cartesianProduct(...rest) {
    return fp.reduce((a, b) => fp.flatMap(x => fp.map(y => x.concat([y]))(b))(a))([[]])(rest);
}

function getPackageVersion() {
    const contents = fs.readFileSync("package.json", "utf8");
    const json = JSON.parse(contents);
    return json.version;
}

async function getVersion() {
    const { stdout, stderr } = await exec("git describe --tags").catch(err => ({
        stdout: "",
        stderr: err.message || err.toString(),
    }));
    if (stderr) console.error(stderr.trim());
    const gitTag = stdout ? _.first(stdout.split(os.EOL)) : null;
    return gitTag || getPackageVersion();
}

module.exports = {
    output,
    debug,
    debugInspect,
    repeat,
    base64Encode,
    inspect,
    interpolate,
    getOrThrow,
    cartesianProduct,
    getVersion,
};
