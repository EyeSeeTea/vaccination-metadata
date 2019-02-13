const _ = require("lodash");
const {interpolate, get, debug, inspect, cartesianProduct} = require("./utils");
const {Db} = require("./db");

const models = [
    {name: "organisationUnits", fields: ["id", "name", "level"]},
    "organisationUnitLevels",

    "categories",
    { name: "categoryCombos", fields: ["id", "name", "categoryOptionCombos"] },
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
                .value())
        .value();
}

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
        return _(innerObject)
            .map((value, key) => _(value).has("key") ? value : ({...value, key}))
            .value();
    }
}

function getIds(objs) {
    return objs.map(obj => ({id: obj.id}));
}

async function getPayloadFromDb(db, sourceData) {
    const categoryOptionsAntigens = toKeyList(sourceData, "antigens").map(antigen => {
        return db.get("categoryOptions", {
            name: antigen.name,
            code: antigen.code,
            shortName: antigen.name,
        });
    });

    const categoryAntigens = db.get("categories", {
        key: "antigens",
        name: "Antigens",
        code: "RVC_ANTIGENS",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: false,
        categoryOptions: getIds(categoryOptionsAntigens),
    });

    const categoriesAntigensMetadata = getCategoriesMetadataForAntigens(db, sourceData);

    const categoriesMetadata = addCategoryOptionCombos(db,
        getCategoriesMetadata(sourceData, db, categoriesAntigensMetadata))

    const dataElementsMetadata = getDataElementsMetadata(db, sourceData, categoriesMetadata);

    const indicatorsMetadata = getIndicatorsMetadata(db, sourceData, dataElementsMetadata);

    const orgUnitsMetadata = getTestOrgUnitsMetadata(db, sourceData)

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole => {
        return db.get("userRoles", userRole);
    });

    const dataSetsMetadata = getDataSetsMetadata(db, sourceData,
        categoriesMetadata, dataElementsMetadata, orgUnitsMetadata);

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

function getDataSetsMetadata(db, sourceData, categoriesMetadata, dataElementsMetadata, orgUnitsMetadata) {
    const organisationUnits = orgUnitsMetadata.organisationUnits;
    const dataElementsById = _.keyBy(dataElementsMetadata.dataElements, "id");
    const dataElementsByKey = _.keyBy(dataElementsMetadata.dataElements, "key");
    const dataElementGroupsByKey = _.keyBy(dataElementsMetadata.dataElementGroups, "key");
    const cocsByCategoryComboId = _.groupBy(categoriesMetadata.categoryOptionCombos, coc => coc.categoryCombo.id);

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
            
            const greyedFields = _(dataElementsByKey)
                .at($section.$greyedDataElements || [])
                .flatMap(dataElement => {
                    const cocs = _.flatten(get(cocsByCategoryComboId, dataElement.categoryCombo.id));
                    return cocs.map(coc => ({
                        dataElement: {id: dataElement.id},
                        categoryOptionCombo: {id: coc.id},
                    }));
                });

            return db.getByKey("sections", {
                ...$section,
                greyedFields,
                dataElements: getIds(dataElements),
            });
        });

        const {keys, levels} = $dataSet.$organisationUnits;
        const organisationUnitsForDataSet = organisationUnits.filter(orgUnit => {
            return _(keys).includes(orgUnit.key) || _(levels).includes(orgUnit.level);
        });

        const dataSet = db.get("dataSets", {
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
            sections: sections.map(section => ({
                ...section,
                dataSet: {id: dataSet.id},
            })),
        };
    }));
}

function getOrgUnitsFromTree(db, parentOrgUnit, orgUnitsByKey) {
    return _(orgUnitsByKey).map((values, key) => ({...values, key})).flatMap(attributes => {
        const orgUnit = db.get("organisationUnits", {
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

    const organisationUnitLevels = toKeyList(sourceData, "organisationUnitLevels").map(orgUnitLevel => {
        return db.get("organisationUnitLevels", orgUnitLevel, {field: "level"});
    });

    return {organisationUnits, organisationUnitLevels};
}

function getCategoriesMetadataForAntigens(db, sourceData) {
    return flattenPayloads(toKeyList(sourceData, "antigens").map(antigen => {
        const categoryOptionsForAge = antigen.ageGroups.map(ageGroupName => {
            return db.get("categoryOptions", {
                name: ageGroupName,
                shortName: ageGroupName,
            });
        });

        const categoryAgeGroup = db.get("categories", {
            key: `age-group-${antigen.key}`,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: true,
            categoryOptions: getIds(categoryOptionsForAge),
        });

        const categoryComboAge = db.get("categoryCombos", {
            key: `age-group-${antigen.key}`,
            code: antigen.code,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            categories: getIds([categoryAgeGroup]),
        });

        return {
            categories: [categoryAgeGroup],
            categoryOptions: categoryOptionsForAge,
            categoryCombos: [categoryComboAge],
        };
    }));
}

function interpolateObj(attributes, namespace) {
    return _(attributes)
        .mapValues(value => {
            if (_(value).isString()) {
                return interpolate(value, namespace);
            } else if (_(value).isArray()) {
                return value.map(v => interpolate(v, namespace))
            } else if(_(value).isObject()) {
                return interpolateObj(value, namespace);
            } else if (_(value).isBoolean()) {
                return value;
            } else {
                throw `Unsupported interpolation object: ${inspect(value)}`;
            }
        }).value();
}

function getIndicator(db, indicatorTypesByKey, namespace, plainAttributes) {
    const attributes = interpolateObj(plainAttributes, namespace);
    
    return db.get("indicators", {
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
                const dataElementInterpolated = interpolateObj(dataElement, {antigen});

                return db.get("dataElements", {
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

            const dataElementGroup = db.get("dataElementGroups", {
                name: dataElement.name,
                key: dataElement.key,
                dataElements: getIds(dataElements),
            });

            return {dataElementGroups: [dataElementGroup], dataElements};
        } else {
            const dataElementDb = db.get("dataElements", {
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
            return db.get("dataElementGroups", {
                name: antigen.name,
                key: antigenKey,
                code: antigen.code,
                dataElements: getIds(dataElementsForAntigen),
            });
        })
        .value();

    const dataElementGroupSet = db.get("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataDimension: false,
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
        .map(indicatorType => db.get("indicatorTypes", indicatorType))
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
                    name: `${indicator.name} - ${antigen.name}`,
                    shortName: `${antigen.shortName || antigen.name} ${indicator.shortName || indicator.name}`,
                    code: `${indicator.code}_${antigen.code}`,
                    domainType: "AGGREGATE",
                    aggregationType: "SUM",
                });
            });

            const indicatorGroup = db.get("indicatorGroups", {
                name: indicator.name,
                indicators: getIds(indicators),
            });

            return {indicatorGroups: [indicatorGroup], indicators: indicators};
        } else {
            const indicatorDb = getIndicator(db, indicatorTypesByKey, namespace, {
                shortName: indicator.shortName || indicator.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                ...indicator,
            });

            return {indicators: [indicatorDb]};
        }
    }));

    const indicatorGroupSet = db.get("indicatorGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        indicatorGroups: getIds(indicatorsMetadata.indicatorGroups),
    });

    const groupsMetadata = {
        indicatorGroupSets: [indicatorGroupSet],
        indicatorTypes: _.values(indicatorTypesByKey),
    };

    return flattenPayloads([indicatorsMetadata, groupsMetadata]);
}

function getCategoriesMetadata(sourceData, db, categoriesAntigensMetadata) {
    const customMetadata = flattenPayloads(toKeyList(sourceData, "categories").map(attributes => {
        const categoryOptions = get(attributes, "$categoryOptions").map(name => {
            return db.get("categoryOptions", {
                name: name,
                shortName: name,
            });
        });

        const category = db.get("categories", {
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: false,
            categoryOptions: getIds(categoryOptions),
            ...attributes,
        });

        const categoryCombo = db.get("categoryCombos", {
            key: attributes.key,
            name: attributes.name,
            code: attributes.code,
            dataDimensionType: attributes.dataDimensionType || "DISAGGREGATION",
            categories: getIds([category]),
        });

        return {
            categories: [category],
            categoryOptions,
            categoryCombos: [categoryCombo],
        };
    }));

    const payload = flattenPayloads([categoriesAntigensMetadata, customMetadata]);

    const categoryCombos = _(toKeyList(sourceData, "categoryCombos")).flatMap(categoryCombo => {
        const categoryCombos = categoryCombo.$byAntigen
            ? toKeyList(sourceData, "antigens").map(antigen => interpolateObj(categoryCombo, { antigen }))
            : [categoryCombo]

        return categoryCombos.map(categoryCombo => {
            const categoriesForCatCombo =
                _(payload.categories).keyBy("key").at(categoryCombo.$categories).value();

            return db.get("categoryCombos", {
                dataDimensionType: "DISAGGREGATION",
                categories: getIds(categoriesForCatCombo),
                ...categoryCombo,
            });
        });
    }).value();

    return flattenPayloads([payload, {categoryCombos}])
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
        /*
        const res = await db.updateCOCs();
        if (res.status < 200 || res.status > 299) {
            throw `Error upding category option combo: ${inspect(res)}`;
        }
        */
    }
    return responseJson;
}

module.exports = {getPayload, postPayload};