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

module.exports = {
    flattenPayloads,
    interpolateObj,
    getName,
    getCode,
    toKeyList,
    getIds,
    addCategoryOptionCombos,
};
