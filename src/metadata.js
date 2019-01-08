const _ = require("lodash");
const {interpolate, get, debug, inspect} = require("./utils");
const {Db} = require("./db");

const models = [
    "organisationUnits",
    "organisationUnitLevels",

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

    "dataSets",
    "sections",
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

function toKeyList(object, path) {
    if (!object) {
        throw new Error("No object");
    } else if (!_.has(object, path)) {
        throw new Error(`Path ${path} not found in object:\n${inspect(object)}`);
    } else {
        const innerObject =_.get(object, path);
        return _(innerObject).map((value, key) => ({...value, key})).value();
    }
}

function getIds(objs) {
    return objs.map(obj => ({id: obj.id}));
}

async function getPayloadFromDb(db, sourceData) {
    const categoryOptionsAntigens = toKeyList(sourceData, "antigens").map(antigen => {
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

    const categoriesMetadata = flattenPayloads([
        ...getCustomCategories(sourceData, db),
        ...getCategoriesMetadataForAntigens(db, sourceData),
    ]);

    const dataElementsMetadata = getDataElementsMetadata(db, sourceData, categoriesMetadata);

    const indicatorsMetadata = getIndicatorsMetadata(db, sourceData, dataElementsMetadata);

    const orgUnitsMetadata = getOrgUnitsMetadata(db, sourceData)

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole => {
        return db.getByName("userRoles", userRole);
    });

    const dataSetsMetadata = getDataSetsMetadata(db, sourceData, dataElementsMetadata);

    const payloadBase = {
        categories: [categoryAntigens],
        categoryOptions: categoryOptionsAntigens,
        userRoles,
    };

    return flattenPayloads([
        payloadBase,
        dataElementsMetadata,
        indicatorsMetadata,
        categoriesMetadata,
        orgUnitsMetadata,
        dataSetsMetadata,
    ]);
}

function getDataSetsMetadata(db, sourceData, dataElementsMetadata) {
    const organisationUnits = db.getObjectsForModel("organisationUnits");
    const dataElementsByKey = _.keyBy(dataElementsMetadata.dataElements, "key");

    const dataSets = toKeyList(sourceData, "dataSets").map($dataSet => {
        const sections = toKeyList($dataSet, "$sections").map($section => {
            return db.getByName("sections", {
                ...$section,
                dataElements: getIds(_(dataElementsByKey).at($section.$dataElements).value()),
            });
        });

        const organisationUnitsForDataSet = organisationUnits.filter(orgUnit => {
            const { names, levels } = $dataSet.$organisationUnitsLevels;
            return _(names).includes(orgUnit.name) || _(levels).includes(orgUnit.level);
        });

        const dataSet = db.getByName("dataSets", {
            ...$dataSet,
            sections: getIds(sections),
            organisationUnits: getIds(organisationUnitsForDataSet),
        });

        const dataSetElements = _(dataElementsByKey)
            .at($dataSet.$dataElements)
            .map(dataElement => ({
                dataElement: {id: dataElement.id},
                dataSet: {id: dataSet.id},
                categoryCombo: {id: dataElement.categoryCombo.id}
            }))
            .value();

        return {...dataSet, dataSetElements};

    });
    return {dataSets};
}

function getOrgUnitsFromTree(db, parentOrgUnit, orgUnitsByKey) {
    return _(orgUnitsByKey).map((values, key) => ({...values, key})).flatMap(attributes => {
        const orgUnit = db.getByName("organisationUnits", {
            level: parentOrgUnit ? parentOrgUnit.level + 1 : 1,
            shortName: attributes.name,
            openingDate: "1970-01-01T00:00:00.000",
            parent: parentOrgUnit ? {id: parentOrgUnit.id} : undefined,
            ..._.omit(attributes, ["children"]),
        });
        const childrenOrgUnits = attributes.children
            ? getOrgUnitsFromTree(db, orgUnit, attributes.children)
            : [];

        return [orgUnit, ...childrenOrgUnits]
    }).value();
}

function getOrgUnitsMetadata(db, sourceData) {
    const organisationUnits = getOrgUnitsFromTree(db, null, {root: sourceData.testing.organisationUnits});
    return {organisationUnits};
}

function getCategoriesMetadataForAntigens(db, sourceData) {
    return toKeyList(sourceData, "antigens").map(antigen => {
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

        const categoryComboAge = db.getByName("categoryCombos", {
            key: `age-group-${antigen.key}`,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            categories: getIds([categoryAgeGroup]),
        });

        return {
            categories: [categoryAgeGroup],
            categoryOptions: categoryOptionsForAge,
            categoryCombos: [categoryComboAge],
        };
    });
}

function getIndicator(db, indicatorTypesByKey, namespace, plainAttributes) {
    const attributes = _(plainAttributes).mapValues(s => interpolate(s, namespace)).value();
    
    return db.getByName("indicators", {
        shortName: attributes.name,
        indicatorType: {
            id: get(indicatorTypesByKey, [attributes.$indicatorType, "id"]),
        },
        ...attributes,
    });
}

function getDataElementsMetadata(db, sourceData, categoriesMetadata) {
    const categoryCombosByName = _.keyBy(db.getObjectsForModel("categoryCombos"), "name");
    const categoryCombosByKey = _.keyBy(categoriesMetadata.categoryCombos, "key");
    
    const getCategoryComboId = (dataElement) => {
        return dataElement.$disaggregation
            ? get(categoryCombosByKey, [dataElement.$disaggregation, "id"])
            : get(categoryCombosByName, ["default", "id"]);
    };

    const dataElementsMetadata = flattenPayloads(toKeyList(sourceData, "dataElements").map(dataElement => {
        if (dataElement.$byAntigen) {
            const dataElements = toKeyList(sourceData, "antigens").map(antigen => {
                const dataElementInterpolated = _(dataElement)
                    .mapValues(s => interpolate(s, {antigen}))
                    .value();

                return db.getByName("dataElements", {
                    ...dataElement,
                    key: `${dataElement.key}-${antigen.key}`,
                    name: `${antigen.name} ${dataElement.name}`,
                    shortName: `${antigen.shortName || antigen.name} ${dataElement.shortName || dataElement.name}`,
                    code: `${dataElement.code}_${antigen.code}`,
                    domainType: "AGGREGATE",
                    aggregationType: "SUM",
                    categoryCombo: {id: getCategoryComboId(dataElementInterpolated)},
                }, {antigen});
            });

            const dataElementGroup = db.getByName("dataElementGroups", {
                name: dataElement.name,
                key: dataElement.key,
                dataElements: getIds(dataElements),
            });

            return {dataElementGroups: [dataElementGroup], dataElements};
        } else {
            const dataElementDb = db.getByName("dataElements", {
                shortName: dataElement.shortName || dataElement.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                categoryCombo: {id: getCategoryComboId(dataElement)},
                ...dataElement,
            });

            return {dataElements: [dataElementDb]};
        }
    }));

    const dataElementGroupSet = db.getByName("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataElementGroups: getIds(dataElementsMetadata.dataElementGroups),
    });

    return flattenPayloads([
        dataElementsMetadata,
        {dataElementGroupSets: [dataElementGroupSet]},
    ]);
}

function getIndicatorsMetadata(db, sourceData, dataElementsMetadata) {
    const indicatorTypesByKey = _(toKeyList(sourceData, "indicatorTypes"))
        .map(indicatorType => db.getByName("indicatorTypes", indicatorType))
        .keyBy("key")
        .value();

    const namespace = {
        dataElements: _.keyBy(dataElementsMetadata.dataElements, "key"),
        dataElementGroups: _.keyBy(dataElementsMetadata.dataElementGroups, "key"),
    }

    const indicatorsMetadata = flattenPayloads(toKeyList(sourceData, "indicators").map(indicator => {
        if (indicator.$byAntigen) {
            const indicators = toKeyList(sourceData, "antigens").map(antigen => {
                return getIndicator(db, indicatorTypesByKey, {...namespace, antigen}, {
                    ...indicator,
                    key: `${indicator.key}-${antigen.key}`,
                    name: `${antigen.name} ${indicator.name}`,
                    shortName: `${antigen.shortName || antigen.name} ${indicator.shortName || indicator.name}`,
                    code: `${indicator.code}_${antigen.code}`,
                    domainType: "AGGREGATE",
                    aggregationType: "SUM",
                });
            });

            const indicatorGroup = db.getByName("indicatorGroups", {
                name: indicator.name,
                indicators: getIds(indicators),
            });

            return {indicatorGroups: [indicatorGroup], indicators: indicators};
        } else {
            const indicatorDb = getIndicator(db, indicatorTypesByKey, antigen, {
                shortName: indicator.shortName || indicator.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                ...indicator,
            });

            return {indicators: [indicatorDb]};
        }
    }));

    const indicatorGroupSet = db.getByName("indicatorGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        indicatorGroups: getIds(indicatorsMetadata.indicatorGroups),
    });

    return flattenPayloads([
        indicatorsMetadata,
        {indicatorGroupSets: [indicatorGroupSet], indicatorTypes: _.values(indicatorTypesByKey)},
    ]);
}

function getCustomCategories(sourceData, db) {
    return toKeyList(sourceData, "categories").map(info => {
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

        return {
            categories: [category],
            categoryOptions,
            categoryCombos: [categoryCombo],
        };
    });
}

/* Public interface */

/* Return JSON payload metadata from source data for a specific DHIS2 instance */
async function getPayload(url, sourceData) {
    const db = await Db.init(url, {models});
    return getPayloadFromDb(db, sourceData);
}

/* POST JSON payload metadata to a DHIS2 instance */
async function postPayload(url, payload, {updateCOCs = false} = {}) {
    const db = await Db.init(url);
    const responseJson = await db.postMetadata(payload);

    if (responseJson.status === "OK" && updateCOCs) {
        debug("Update category option combinations");
        await db.updateCOCs();
    }
    return responseJson;
}

module.exports = {getPayload, postPayload};