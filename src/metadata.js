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
        const innerObject = {
            ..._.get(object, path),
            ..._.get(object, "test." + path),
        };
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

    const orgUnitsMetadata = getTestOrgUnitsMetadata(db, sourceData)

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole => {
        return db.getByName("userRoles", userRole);
    });

    const dataSetsMetadata = getDataSetsMetadata(db, sourceData,  categoriesMetadata, dataElementsMetadata);

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

function getDataSetsMetadata(db, sourceData, categoriesMetadata, dataElementsMetadata) {
    const organisationUnits = db.getObjectsForModel("organisationUnits");
    const dataElementsById = _.keyBy(dataElementsMetadata.dataElements, "id");
    const dataElementsByKey = _.keyBy(dataElementsMetadata.dataElements, "key");
    const dataElementGroupsByKey = _.keyBy(dataElementsMetadata.dataElementGroups, "key");

    return flattenPayloads(toKeyList(sourceData, "dataSets").map($dataSet => {
        const sections = toKeyList($dataSet, "$sections").map($section => {
            const dataElements = _.concat(
                _(dataElementsByKey).at($section.$dataElements || []).value(),
                _(dataElementGroupsByKey)
                    .at($section.$dataElementsByGroups || [])
                    .flatMap("dataElements")
                    .map(de => dataElementsById[de.id])
                    .value(),
            );
            return db.getByKey("sections", {
                ...$section,
                dataElements: getIds(dataElements),
            });
        });

        const organisationUnitsForDataSet = organisationUnits.filter(orgUnit => {
            const { names, levels } = $dataSet.$organisationUnits;
            return _(names).includes(orgUnit.name) || _(levels).includes(orgUnit.level);
        });

        const dataSet = db.getByName("dataSets", {
            ...$dataSet,
            categoryCombo: getCategoryComboId(db, categoriesMetadata, $dataSet),
            organisationUnits: getIds(organisationUnitsForDataSet),
        });

        const dataSetElements = _(sections)
            .flatMap("dataElements")
            .map(de => dataElementsById[de.id])
            .map(dataElement => ({
                dataElement: {id: dataElement.id},
                dataSet: {id: dataSet.id},
                categoryCombo: {id: dataElement.categoryCombo.id}
            }))
            .value();

        return {
            dataSets: [{...dataSet, dataSetElements}],
            _sections: sections.map((section, idx) => ({
                ...section,
                sortOrder: idx,
                dataSet: {id: dataSet.id},
            })),
        };
    }));
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

function getTestOrgUnitsMetadata(db, sourceData) {
    const organisationUnits = getOrgUnitsFromTree(db, null, {root: sourceData.test.organisationUnits});
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

function getCategoryComboId(db, categoriesMetadata, obj) {
    const categoryCombosByName = _.keyBy(db.getObjectsForModel("categoryCombos"), "name");
    const categoryCombosByKey = _.keyBy(categoriesMetadata.categoryCombos, "key");
    const ccId = obj.$categoryCombo
        ? get(categoryCombosByKey, [obj.$categoryCombo, "id"])
        : get(categoryCombosByName, ["default", "id"]);
    return {id: ccId};
}

function getDataElementsMetadata(db, sourceData, categoriesMetadata) {
    const dataElementsMetadata = flattenPayloads(toKeyList(sourceData, "dataElements").map(dataElement => {
        if (dataElement.$byAntigen) {
            const dataElements = toKeyList(sourceData, "antigens").map(antigen => {
                const dataElementInterpolated = _(dataElement)
                    .mapValues(s => interpolate(s, {antigen}))
                    .value();

                return db.getByName("dataElements", {
                    ...dataElement,
                    key: `${dataElement.key}-${antigen.key}`,
                    name: `${dataElement.name} - ${antigen.name}`,
                    shortName: `${antigen.shortName || antigen.name} ${dataElement.shortName || dataElement.name}`,
                    code: `${dataElement.code}_${antigen.code}`,
                    domainType: "AGGREGATE",
                    aggregationType: "SUM",
                    categoryCombo: getCategoryComboId(db, categoriesMetadata, dataElementInterpolated),
                    $antigen: antigen,
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
                categoryCombo: getCategoryComboId(db, categoriesMetadata, dataElement),
                ...dataElement,
            });

            return {dataElements: [dataElementDb]};
        }
    }));

    const dataElementGroupsForAntigens = _(dataElementsMetadata.dataElements)
        .filter("$antigen")
        .groupBy(dataElement => dataElement.$antigen.key)
        .map((dataElementsForAntigen, antigenKey) => {
            const antigen = sourceData.antigens[antigenKey];
            return db.getByName("dataElementGroups", {
                name: antigen.name,
                key: antigenKey,
                dataElements: getIds(dataElementsForAntigen),
            });
        })
        .value();

    const dataElementGroupSet = db.getByName("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataElementGroups: getIds(dataElementsMetadata.dataElementGroups),
    });

    return flattenPayloads([
        dataElementsMetadata,
        {
            dataElementGroups: dataElementGroupsForAntigens,
            dataElementGroupSets: [dataElementGroupSet],
        },
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
        const res = await db.updateCOCs();
        if (res.status < 200 || res.status > 299) {
            throw `Error upding category option combo: ${inspect(res)}`;
        }
    }
    return responseJson;
}

module.exports = {getPayload, postPayload};