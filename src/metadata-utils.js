const _ = require("lodash");
const { interpolate, getOrThrow, cartesianProduct, inspect } = require("./utils");

function getCode(parts) {
    return parts.map(part => part.replace(/\s*/g, "").toUpperCase()).join("_");
}

function getName(parts) {
    return parts.join(" - ");
}

/* Transform an array of {key1: [...], key2: [...]} into a single accumulated object */
function flattenPayloads(payloads) {
    return _(payloads)
        .map(_.toPairs)
        .flatten()
        .groupBy(([key, _values]) => key)
        .mapValues(pairs =>
            _(pairs)
                .flatMap(([_key, values]) => values)
                .uniqBy("id")
                .value()
        )
        .value();
}

function interpolateObj(attributes, namespace) {
    return _.mapValues(attributes, value => {
        if (_(value).isString()) {
            return interpolate(value, namespace);
        } else if (_(value).isArray()) {
            return value.map(v => interpolate(v, namespace));
        } else if (_(value).isObject()) {
            return interpolateObj(value, namespace);
        } else if (_(value).isBoolean()) {
            return value;
        } else {
            throw `Unsupported interpolation object: ${inspect(value)}`;
        }
    });
}

function toKeyList(object, path) {
    if (!object) {
        throw new Error("No object");
    } else if (!_.has(object, path)) {
        throw new Error(`Path ${path} not found in object:\n${inspect(object)}`);
    } else {
        return _(getOrThrow(object, path))
            .map((value, key) => (_(value).has("key") ? value : { ...value, key }))
            .value();
    }
}

function getIds(objs) {
    return objs.map(obj => ({ id: getOrThrow(obj, "id") }));
}

function addCategoryOptionCombos(db, payload) {
    const { categories, categoryOptions, categoryCombos } = payload;
    const categoriesById = _.keyBy(categories, "id");
    const categoryOptionsById = _.keyBy(categoryOptions, "id");

    const categoryOptionCombos = _(categoryCombos)
        .flatMap(categoryCombo => {
            const categoryOptionsList = _(categoryCombo.categories)
                .map(category =>
                    getOrThrow(categoriesById, [category.id, "categoryOptions"]).map(co => co.id)
                )
                .value();

            return cartesianProduct(...categoryOptionsList).map(categoryOptionIds => {
                const categoryOptions = _(categoryOptionsById)
                    .at(categoryOptionIds)
                    .value();
                const key = [categoryCombo.id, ...categoryOptionIds].join(".");

                return db.getByKey("categoryOptionCombos", {
                    key: key,
                    name: _(categoryOptions)
                        .map("name")
                        .join(", "),
                    categoryCombo: { id: categoryCombo.id },
                    categoryOptions: categoryOptionIds.map(id => ({ id })),
                });
            });
        })
        .value();

    return { ...payload, categoryOptionCombos };
}

function sortAgeGroups(names) {
    const timeUnits = { d: 1, w: 7, m: 30, y: 365 };

    return _.sortBy(names, name => {
        const parts = name.split(" ");
        let pair;
        if (parts.length === 4) {
            // "2 - 5 y"
            const days = getOrThrow(timeUnits, parts[3]);
            pair = [parseInt(parts[0]) * days, parseInt(parts[2]) * days];
        } else if (parts.length === 5) {
            // "2 w - 3 y" {
            const days1 = getOrThrow(timeUnits, parts[1]);
            const days2 = getOrThrow(timeUnits, parts[4]);
            pair = [parseInt(parts[0]) * days1, parseInt(parts[3]) * days2];
        } else if (parts.length === 3) {
            // ""> 30 y"
            const days = getOrThrow(timeUnits, parts[2]);
            pair = [parseInt(parts[1]) * days, 0];
        } else {
            throw new Error(`Invalid age range format: ${name}`);
        }

        return 100000 * pair[0] + pair[1];
    });
}

module.exports = {
    flattenPayloads,
    interpolateObj,
    getName,
    getCode,
    toKeyList,
    getIds,
    addCategoryOptionCombos,
    sortAgeGroups,
};
