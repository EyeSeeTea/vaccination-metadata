const _ = require("lodash");
const {interpolate, get, cartesianProduct, inspect} = require("./utils");
const {Db} = require("./db");

const models = [
    "categories",
    "categoryCombos",
    "categoryOptions",
    "categoryOptionCombos",

    "dataElementGroupSets",
    "dataElementGroups",
    "dataElements",

    "indicatorTypes",
    "indicatorGroupSets",
    "indicatorGroups",
    "indicators",

    "userRoles",
];

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
                //.map(obj => _.omit(obj, ["key"]))
                .value())
        .value();
}

function toKeyList(object) {
    if (object) {
        return _(object).map((value, key) => ({...value, key})).value();
    } else {
        throw new Error("No object");
    }
}


function getIds(objs) {
    return objs.map(obj => ({id: obj.id}));
}

/* categoryOptionsCombos are not created automatically, so we'll generate them from
   the payload, using categories, categoryCombos and a cartessian product of categoryOptions. */
function addCategoryOptionCombos(db, payload) {
    const {categories, categoryOptions, categoryCombos} = payload;
    const categoriesById = _.keyBy(categories, "id");
    const categoryOptionsById = _.keyBy(categoryOptions, "id");

    const categoryOptionCombos = _(categoryCombos).flatMap(categoryCombo => {
        const categoryOptionsList = _(categoryCombo.categories)
            .map(category => get(categoriesById, [category.id, "categoryOptions"]).map(co => co.id))
            .value();

        return cartesianProduct(...categoryOptionsList).map(categoryOptionIds => {
            const categoryOptions = _(categoryOptionsById).at(categoryOptionIds).value();
            const key = [categoryCombo.id, ...categoryOptionIds].join(".");

            return db.getByKey("categoryOptionCombos", {
                key: key,
                name: _(categoryOptions).map("name").join(", "),
                categoryCombo: {id: categoryCombo.id},
                categoryOptions: categoryOptionIds.map(id => ({id})),
            });
        });
    }).value();

    return {...payload, categoryOptionCombos};
}

async function getPayloadFromDb(db, sourceData) {
    const categoryOptionsAntigens = toKeyList(sourceData.antigens).map(antigen => {
        return db.getByName("categoryOptions", {
            name: antigen.name,
            code: antigen.code,
            shortName: antigen.name,
        });
    });

    const categoryAntigens = db.getByName("categories", {
        key: "antigens",
        name: "Antigens",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: true,
        categoryOptions: getIds(categoryOptionsAntigens),
    });

    const categoryOptionsGender = ["Female", "Male"].map(name => {
        return db.getByName("categoryOptions", {name, shortName: name});
    });

    const categoryGender = db.getByName("categories", {
        key: "gender",
        name: "Gender",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: false,
        categoryOptions: getIds(categoryOptionsGender),
    });

    const customCategoriesMetadataByKey = getCustomCategories(sourceData, db);

    const categoriesMetadataByAntigen = getCategoriesMetadataByAntigen(db, sourceData, categoryGender);

    const dataElementsMetadata = getDataElementsMetadata(
        db,
        sourceData,
        categoriesMetadataByAntigen,
        customCategoriesMetadataByKey,
    );

    const indicatorsMetadata = getIndicatorsMetadata(
        db,
        sourceData,
        dataElementsMetadata,
    );

    const userRoles = toKeyList(sourceData.userRoles).map(userRole => {
        return db.getByName("userRoles", userRole);
    });

    const payloadBase = {
        categories: [categoryGender, categoryAntigens],
        categoryOptions: [...categoryOptionsGender, ...categoryOptionsAntigens],
        userRoles,
    };

    const payload = flattenPayloads([
        payloadBase,
        dataElementsMetadata,
        indicatorsMetadata,
        ..._.values(customCategoriesMetadataByKey),
        ..._.values(categoriesMetadataByAntigen),
    ]);

    return addCategoryOptionCombos(db, payload);
}

function getCategoriesMetadataByAntigen(db, sourceData, categoryGender) {
    return _(toKeyList(sourceData.antigens)).map(antigen => {
        const categoryOptionsForAge = antigen.ageGroups.map(ageGroupName => {
            return db.getByName("categoryOptions", {
                name: ageGroupName,
                shortName: ageGroupName,
            });
        });
        const categoryAgeGroup = db.getByName("categories", {
            key: `age-group-${antigen.key}`,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: true,
            categoryOptions: getIds(categoryOptionsForAge),
        });
        const categoryComboAgeGender = db.getByName("categoryCombos", {
            key: `age-group-gender-${antigen.key}`,
            name: `Age group / Gender ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            categories: getIds([categoryAgeGroup, categoryGender]),
        });
        const data = {
            categories: [categoryAgeGroup],
            categoryOptions: categoryOptionsForAge,
            categoryCombos: [categoryComboAgeGender],
        };
        return [antigen.key, data];
    }).fromPairs().value();
}

function getIndicator(db, info, indicatorTypesByKey, namespace) {
    const infoInterpolated = _(info)
        .mapValues(s => interpolate(s, namespace))
        .value();

    return db.getByName("indicators", {
        shortName: info.name,
        ...infoInterpolated,
        indicatorType: {
            id: get(indicatorTypesByKey, [info.indicatorType, "id"]),
        },
    });
}

function getDataElementsMetadata(db, sourceData, categoriesMetadataByAntigen, customCategoriesMetadataByKey) {
    const categoryCombosByName = _(db.getObjectsForModel("categoryCombos"))
        .keyBy("name")
        .value();

    const dataElementsGlobal = toKeyList(sourceData.vaccinationData.global).map(info => {
        return db.getByName("dataElements", {
            shortName: info.name,
            domainType: "AGGREGATE",
            aggregationType: "SUM",
            categoryCombo: {
                id: info.disaggregation
                    ? get(customCategoriesMetadataByKey, [info.disaggregation, "categoryCombos", 0, "id"])
                    : get(categoryCombosByName, ["default", "id"])
            },
            ...info,
        });
    });

    const dataByAntigen = toKeyList(sourceData.vaccinationData.byAntigen);

    const dataElementsByAntigenMetadata = dataByAntigen.map(group => {
        const dataElements = toKeyList(sourceData.antigens).map(antigen => {
            const groupItemsInterpolated = _(group.items)
                .mapValues(s => interpolate(s, {antigen}))
                .value();

            return db.getByName("dataElements", {
                ...groupItemsInterpolated,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                categoryCombo: {id: get(categoriesMetadataByAntigen, [antigen.key, "categoryCombos", 0, "id"])},
            });
        });

        const dataElementGroup = db.getByName("dataElementGroups", {
            ..._(group).omit(["items"]).value(),
            dataElements: getIds(dataElements),
        });

        return {dataElementGroups: [dataElementGroup], dataElements};
    });

    const payload = flattenPayloads([
        ...dataElementsByAntigenMetadata,
        {dataElements: dataElementsGlobal},
    ]);

    const dataElementGroupSet = db.getByName("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataElementGroups: getIds(payload.dataElementGroups),
    });

    return flattenPayloads([payload, {dataElementGroupSets: [dataElementGroupSet]}]);
}

function getIndicatorsMetadata(db, sourceData, dataElementsMetadata) {
    const indicatorTypesByKey = _(toKeyList(sourceData.indicatorTypes))
        .map(indicatorType => db.getByName("indicatorTypes", indicatorType))
        .keyBy("key")
        .value();

    const namespace = {
        dataElements: _.keyBy(dataElementsMetadata.dataElements, "key"),
        dataElementGroups: _.keyBy(dataElementsMetadata.dataElementGroups, "key"),
    }

    const indicatorsGlobal = toKeyList(sourceData.vaccinationIndicators.global).map(info => {
        return getIndicator(db, info, indicatorTypesByKey, namespace);
    });

    const indicatorsMetadataByAntigen = toKeyList(sourceData.vaccinationIndicators.byAntigen).map(group => {
        const indicators = toKeyList(sourceData.antigens).map(antigen => {
            return getIndicator(db, group.items, indicatorTypesByKey, {
                ...namespace,
                antigen,
            });
        });
        const indicatorGroup = db.getByName("indicatorGroups", {
            ..._(group).omit(["items"]).value(),
            indicators: getIds(indicators),
        });
        return {indicatorGroups: [indicatorGroup], indicators};
    });

    const payload = flattenPayloads([
        ...indicatorsMetadataByAntigen,
        {indicators: indicatorsGlobal},
    ]);

    const indicatorGroupSet = db.getByName("indicatorGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        indicatorGroups: getIds(payload.indicatorGroups),
    });

    return flattenPayloads([payload, {
        indicatorGroupSets: [indicatorGroupSet],
        indicatorTypes: _.values(indicatorTypesByKey),
    }]);
}

function getCustomCategories(sourceData, db) {
    const pairs = toKeyList(sourceData.categories).map(info => {
        const dataDimensionType = info.dataDimensionType || "DISAGGREGATION";

        const categoryOptions = get(info, "categoryOptions").map(name => {
            return db.getByName("categoryOptions", {
                name: name,
                shortName: name,
            });
        });

        const category = db.getByName("categories", {
            key: info.key,
            name: info.name,
            dataDimensionType,
            dimensionType: "CATEGORY",
            dataDimension: false,
            categoryOptions: getIds(categoryOptions),
        });

        const categoryCombo = db.getByName("categoryCombos", {
            key: info.key,
            name: info.name,
            dataDimensionType,
            categories: getIds([category]),
        });

        const data = {
            categories: [category],
            categoryOptions,
            categoryCombos: [categoryCombo],
        };

        return [info.key, data];
    });

    return _(pairs).fromPairs().value();
}

/* Public interface */

/* Return JSON payload metadata from source data for a specific DHIS2 instance */
async function getPayload(url, sourceData) {
    const db = await Db.init(url, {models});
    return getPayloadFromDb(db, sourceData);
}

/* POST JSON payload metadata to a DHIS2 instance */
async function postPayload(url, payload) {
    const db = await Db.init(url);
    return db.postMetadata(payload);
}

module.exports = {getPayload, postPayload};