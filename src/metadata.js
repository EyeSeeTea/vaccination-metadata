const _ = require("lodash");
const { interpolate, get, debug, inspect, cartesianProduct } = require("./utils");
const { Db } = require("./db");

const models = [
    "attributes",

    { name: "organisationUnits", fields: ["id", "name", "level"] },

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
                .value()
        )
        .value();
}

function addCategoryOptionCombos(db, payload) {
    const { categories, categoryOptions, categoryCombos } = payload;
    const categoriesById = _.keyBy(categories, "id");
    const categoryOptionsById = _.keyBy(categoryOptions, "id");

    const categoryOptionCombos = _(categoryCombos)
        .flatMap(categoryCombo => {
            const categoryOptionsList = _(categoryCombo.categories)
                .map(category =>
                    get(categoriesById, [category.id, "categoryOptions"]).map(co => co.id)
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

function getOrThrow(obj, property, message) {
    if (obj && obj.hasOwnProperty(property)) {
        return obj[property];
    } else {
        throw new Error(message || `Object has no property ${property}: ${inspect(obj)}`);
    }
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
            .map((value, key) => (_(value).has("key") ? value : { ...value, key }))
            .value();
    }
}

function getIds(objs) {
    return objs.map(obj => ({ id: getOrThrow(obj, "id") }));
}

async function getPayloadFromDb(db, sourceData) {
    const categoriesAntigensMetadata = getCategoriesMetadataForAntigens(db, sourceData);

    const categoriesMetadata = addCategoryOptionCombos(
        db,
        flattenPayloads([
            categoriesAntigensMetadata,
            getCategoriesMetadata(sourceData, db, categoriesAntigensMetadata),
        ])
    );

    const dataElementsMetadata = getDataElementsMetadata(db, sourceData, categoriesMetadata);

    const indicatorsMetadata = getIndicatorsMetadata(db, sourceData, dataElementsMetadata);

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole => {
        return db.get("userRoles", userRole);
    });

    const dataSetsMetadata = getDataSetsMetadata(
        db,
        sourceData,
        categoriesMetadata,
        dataElementsMetadata
    );

    const attributes = getAttributes(db, sourceData);

    const payloadBase = {
        userRoles,
        attributes,
    };

    return flattenPayloads([
        payloadBase,
        dataElementsMetadata,
        indicatorsMetadata,
        categoriesMetadata,
        dataSetsMetadata,
    ]);
}

function getAttributes(db, sourceData) {
    return toKeyList(sourceData, "attributes").map(attribute => {
        return db.get("attributes", attribute);
    });
}

function getDataSetsMetadata(db, sourceData, categoriesMetadata, dataElementsMetadata) {
    const organisationUnits = db.getObjectsForModel("organisationUnits"); //.concat(orgUnitsMetadata.organisationUnits);
    const dataElementsById = _.keyBy(dataElementsMetadata.dataElements, "id");
    const dataElementsByKey = _.keyBy(dataElementsMetadata.dataElements, "key");
    const cocsByCategoryComboId = _.groupBy(
        categoriesMetadata.categoryOptionCombos,
        coc => coc.categoryCombo.id
    );

    return flattenPayloads(
        toKeyList(sourceData, "dataSets").map($dataSet => {
            const sections = toKeyList($dataSet, "$sections").map($section => {
                const dataElements = _(dataElementsByKey)
                    .at($section.$dataElements || [])
                    .value();

                const greyedFields = _(dataElementsByKey)
                    .at($section.$greyedDataElements || [])
                    .flatMap(dataElement => {
                        const cocs = _.flatten(
                            get(cocsByCategoryComboId, dataElement.categoryCombo.id)
                        );
                        return cocs.map(coc => ({
                            dataElement: { id: dataElement.id },
                            categoryOptionCombo: { id: coc.id },
                        }));
                    });

                return db.getByKey("sections", {
                    ...$section,
                    greyedFields,
                    dataElements: getIds(dataElements),
                });
            });

            const { keys, levels } = $dataSet.$organisationUnits || [];
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
                    dataElement: { id: dataElement.id },
                    dataSet: { id: dataSet.id },
                    categoryCombo: { id: dataElement.categoryCombo.id },
                }))
                .value();

            return {
                dataSets: [{ ...dataSet, dataSetElements }],
                sections: sections.map(section => ({
                    ...section,
                    dataSet: { id: dataSet.id },
                })),
            };
        })
    );
}

function getCategoriesMetadataForAntigens(db, sourceData) {
    const ageGroups = _(toKeyList(sourceData, "antigens"))
        .map("ageGroups")
        .flattenDeep()
        .uniq()
        .value();

    const categoryOptionsAgeGroups = ageGroups.map(ageGroup => {
        return db.get("categoryOptions", {
            name: ageGroup,
            shortName: ageGroup,
        });
    });

    const categoryOptionsAgeGroupsByName = _.keyBy(categoryOptionsAgeGroups, "name");

    const metadata = flattenPayloads(
        toKeyList(sourceData, "antigens").map(antigen => {
            const categoryOptions = db.get("categoryOptions", {
                name: antigen.name,
                code: antigen.code,
                shortName: antigen.name,
            });

            const ageGroups = get(antigen, "ageGroups");
            const mainAgeGroups = ageGroups.map(group => group[0][0]);
            const name = getName([antigen.name, "Age groups"]);

            const mainGroup = db.get("categoryOptionGroup", {
                name: name,
                code: `${antigen.code}_AGE_GROUPS`,
                shortName: name,
                categoryOptions: getIds(_.at(categoryOptionsAgeGroupsByName, mainAgeGroups)),
            });

            const disaggregatedGroups = _.flatMap(ageGroups, group => {
                const [mainGroupValues, ...restOfAgeGroup] = group;
                if (mainGroupValues.length !== 1)
                    throw "First age group must contain a single element";
                const mainGroup = mainGroupValues[0];

                if (_(restOfAgeGroup).isEmpty()) {
                    return [];
                } else {
                    return restOfAgeGroup.map((options, index) => {
                        const sIndex = (index + 1).toString();
                        const name = getName([antigen.name, "Age group", mainGroup, sIndex]);
                        return db.get("categoryOptionGroup", {
                            name: name,
                            shortName: name,
                            code: getCode([antigen.code, "AGE_GROUP", mainGroup, sIndex]),
                            categoryOptions: getIds(_.at(categoryOptionsAgeGroupsByName, options)),
                        });
                    });
                }
            });

            return {
                categoryOptions,
                categoryOptionGroups: [mainGroup, ...disaggregatedGroups],
            };
        })
    );

    return flattenPayloads([
        metadata,
        {
            categoryOptions: categoryOptionsAgeGroups,
        },
    ]);
}

function interpolateObj(attributes, namespace) {
    return _(attributes)
        .mapValues(value => {
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
        })
        .value();
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
    return { id: ccId };
}

function getCode(parts) {
    return parts.map(part => part.replace(/\s*/g, "").toUpperCase()).join("_");
}

function getName(parts) {
    return parts.join(" - ");
}

function getCategoryCombosForDataElements(db, dataElement, categoriesMetadata) {
    const categoriesByKey = _.keyBy(categoriesMetadata.categories, "key");
    const categoriesForDataElement = dataElement.$categories;

    if (categoriesForDataElement) {
        return _(categoriesForDataElement)
            .partition("optional")
            .map(categories => categories.map(category => get(categoriesByKey, category.key)))
            .zip(["Optional", "Required"])
            .flatMap(([categories, typeString]) => {
                return db.get("categoryCombos", {
                    key: `data-element-${dataElement.key}-${typeString}`,
                    code: `${dataElement.code}_${typeString.toUpperCase()}`,
                    name: getName([dataElement.name, typeString]),
                    dataDimensionType: "DISAGGREGATION",
                    categories: getIds(categories),
                });
            })
            .value();
    } else {
        return [];
    }
}

function getDataElementGroupsForAntigen(db, antigen, dataElements) {
    const dataElementsByKey = _.keyBy(dataElements, "key");

    return _(antigen.dataElements)
        .partition("optional")
        .map(des => des.map(de => get(dataElementsByKey, de.key)))
        .zip(["Optional", "Required"])
        .flatMap(([dataElementsGroup, typeString]) => {
            return db.get("dataElementGroups", {
                key: `data-elements-${antigen.key}-${typeString}`,
                code: `${antigen.code}_${typeString.toUpperCase()}`,
                name: getName([antigen.name, typeString]),
                shortName: getName([antigen.name, "DES", typeString]),
                dataElements: getIds(dataElementsGroup),
            });
        })
        .value();
}

function getDataElementsMetadata(db, sourceData, categoriesMetadata) {
    const dataElementsMetadata = flattenPayloads(
        toKeyList(sourceData, "dataElements").map(dataElement => {
            const dataElements = db.get("dataElements", {
                shortName: dataElement.shortName || dataElement.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                categoryCombo: getCategoryComboId(db, categoriesMetadata, dataElement),
                ...dataElement,
            });

            const categoryCombosForDataElements = getCategoryCombosForDataElements(
                db,
                dataElement,
                categoriesMetadata
            );

            return { dataElements, categoryCombos: categoryCombosForDataElements };
        })
    );

    const dataElementGroupsMetadata = flattenPayloads(
        toKeyList(sourceData, "antigens").map(antigen => {
            const dataElementGroupsForAntigen = getDataElementGroupsForAntigen(
                db,
                antigen,
                dataElementsMetadata.dataElements
            );

            const dataElementGroupSetForAntigen = db.get("dataElementGroupSets", {
                key: `data-elements-${antigen.key}`,
                code: `${antigen.code}`,
                name: antigen.name,
                dataElementGroups: getIds(dataElementGroupsForAntigen),
            });

            return {
                dataElementGroups: dataElementGroupsForAntigen,
                dataElementGroupSets: [dataElementGroupSetForAntigen],
            };
        })
    );

    return flattenPayloads([dataElementGroupsMetadata, dataElementsMetadata]);
}

function getIndicatorsMetadata(db, sourceData, dataElementsMetadata) {
    const indicatorTypesByKey = _(toKeyList(sourceData, "indicatorTypes"))
        .map(indicatorType => db.get("indicatorTypes", indicatorType))
        .keyBy("key")
        .value();

    const namespace = {
        dataElements: _(dataElementsMetadata.dataElements)
            .keyBy("key")
            .mapValues("id")
            .value(),
    };

    const indicatorsMetadata = flattenPayloads(
        toKeyList(sourceData, "indicators").map(indicator => {
            const indicatorDb = getIndicator(db, indicatorTypesByKey, namespace, {
                shortName: indicator.shortName || indicator.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                ...indicator,
            });

            return { indicators: [indicatorDb] };
        })
    );

    const indicatorGroupSet = db.get("indicatorGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        indicatorGroups: getIds(indicatorsMetadata.indicatorGroups || []),
    });

    const groupsMetadata = {
        indicatorGroupSets: [indicatorGroupSet],
        indicatorTypes: _.values(indicatorTypesByKey),
    };

    return flattenPayloads([indicatorsMetadata, groupsMetadata]);
}

function getCategoriesMetadata(sourceData, db, categoriesAntigensMetadata) {
    const customMetadata = flattenPayloads(
        toKeyList(sourceData, "categories").map(attributes => {
            const $categoryOptions = get(attributes, "$categoryOptions");
            const antigenCodes = Object.values(sourceData.antigens).map(antigen => antigen.code);

            const [antigenOptions, ageGroupOptions] = _(categoriesAntigensMetadata.categoryOptions)
                .sortBy("name")
                .partition(categoryOption => antigenCodes.includes(categoryOption.code))
                .value();

            let categoryOptions;
            if ($categoryOptions.kind == "fromAntigens") {
                categoryOptions = antigenOptions;
            } else if ($categoryOptions.kind == "fromAgeGroups") {
                categoryOptions = ageGroupOptions;
            } else {
                categoryOptions = $categoryOptions.values.map(name => {
                    return db.get("categoryOptions", {
                        name: name,
                        shortName: name,
                    });
                });
            }

            const category = db.get("categories", {
                dataDimensionType: "DISAGGREGATION",
                dimensionType: "CATEGORY",
                dataDimension: false,
                categoryOptions: getIds(categoryOptions),
                ...attributes,
            });

            return {
                categories: [category],
                categoryOptions,
            };
        })
    );

    const payload = flattenPayloads([categoriesAntigensMetadata, customMetadata]);

    const categoryCombos = _(toKeyList(sourceData, "categoryCombos"))
        .flatMap(categoryCombo => {
            const categoriesForCatCombo = _(payload.categories)
                .keyBy("key")
                .at(categoryCombo.$categories)
                .compact()
                .value();

            return db.get("categoryCombos", {
                dataDimensionType: "DISAGGREGATION",
                categories: getIds(categoriesForCatCombo),
                ...categoryCombo,
            });
        })
        .value();

    return flattenPayloads([payload, { categoryCombos }]);
}

/* Public interface */

/* Return JSON payload metadata from source data for a specific DHIS2 instance */
async function getPayload(url, sourceData) {
    const db = await Db.init(url, { models });
    return getPayloadFromDb(db, sourceData);
}

/* POST JSON payload metadata to a DHIS2 instance */
async function postPayload(url, payload, { updateCOCs = false } = {}) {
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

module.exports = { getPayload, postPayload };
