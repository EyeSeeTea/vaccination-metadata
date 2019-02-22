const _ = require("lodash");
const pMap = require("p-map");
const md5 = require("md5");
const fetch = require("node-fetch");
const {repeat, inspect} = require("./utils");

// DHIS2 UID :: /^[a-zA-Z]{1}[a-zA-Z0-9]{10}$/
const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const asciiNumbers = "0123456789";
const asciiLettersAndNumbers = asciiLetters + asciiNumbers;
const uidStructure = [asciiLetters, ...repeat(asciiLettersAndNumbers, 10)];
const maxHashValue = _(uidStructure).map(cs => cs.length).reduce((acc, n) => acc * n, 1);

/* Return UID from key */
function getUid(key, prefix) {
    const md5hash = md5(prefix + key);
    const nHashChars = Math.ceil(Math.log(maxHashValue) / Math.log(16));
    const hashInteger = parseInt(md5hash.slice(0, nHashChars), 16);
    const result = uidStructure.reduce((acc, chars) => {
        const {n, uid} = acc;
        const nChars = chars.length;
        const quotient = Math.floor(n / nChars);
        const remainder = n % nChars;
        const uidChar = chars[remainder];
        return {n: quotient, uid: uid + uidChar};
    }, {n: hashInteger, uid: ""});

    return result.uid;
}

class Db {
    constructor(url, data) {
        this.url = url;
        this.data = data;
    }

    static async init(url, {models} = {}) {
        let data;

        if (models) {
            const getExistingAsPairs = async (value) => {
                const {name: model, fields} = value.name
                    ? value
                    : {name: value, fields: ["id", "name"]};

                const json = await fetch(`${url}/api/${model}?fields=${fields.join(",")}&paging=false`)
                    .then(res => res.json());
                return [model, json[model]];
            };
            data = _.fromPairs(await pMap(models, getExistingAsPairs, {concurrency: 2}));
        } else {
            data = {};
        }

        return new Db(url, data);
    }

    getObjectsForModel(model) {
        return this.data[model];
    }

    get(model, allAttributes, {field = "name"} = {}) {
        const {key, ...attributes} = allAttributes;
        const value = attributes[field];
        const valuesByField = _.keyBy(this.data[model], field);

        if (!valuesByField) {
            throw `Model not found in data: ${model}`;
        } else if (!value) {
            throw `Property ${field} is required in attributes: ${inspect(attributes)}`;
        } else {
            const uid = _(valuesByField).get([value, "id"]) ||
                getUid(key || value, model + "-");
            return {...attributes, id: uid, key};
        }
    }

    getByKey(model, allAttributes) {
        const { key, ...attributes } = allAttributes;
        if (!key) {
            throw `Name key is required in attributes: ${inspect(attributes)}`;
        } else {
            const uid = getUid(key, model + "-");
            return {...attributes, id: uid, key};
        }
    }

    async postMetadata(payload) {
        const headers = {"Content-Type": "application/json"};
        const response = await fetch(`${this.url}/api/metadata`, {
            method: "POST",
            body: JSON.stringify(payload),
            headers,
        });
        return response.json();
    }

    async updateCOCs() {
        return fetch(`${this.url}/api/maintenance/categoryOptionComboUpdate`, {
            method: "POST",
        });
    }
}

exports.Db = Db;
