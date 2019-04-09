const _ = require("lodash");
const { Db } = require("./db");
const { getOrThrow, debug } = require("./utils");
const { flattenPayloads, interpolateObj, getName, getCode } = require("./metadata-utils");
const { toKeyList, getIds, addCategoryOptionCombos, sortAgeGroups } = require("./metadata-utils");

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

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole =>
        db.get("userRoles", userRole)
    );

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
                            getOrThrow(cocsByCategoryComboId, dataElement.categoryCombo.id)
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

function getCategoryOptionGroupsForAgeGroups(db, antigen, categoryOptionsAgeGroups) {
    const categoryOptionsAgeGroupsByName = _.keyBy(categoryOptionsAgeGroups, "name");
    const ageGroups = getOrThrow(antigen, "ageGroups");
    const mainAgeGroups = ageGroups.map(group => group[0][0]);
    const name = getName([antigen.name, "Age groups"]);

    const mainGroup = db.get("categoryOptionGroup", {
        name: name,
        code: `${antigen.code}_AGE_GROUP`,
        shortName: name,
        categoryOptions: getIds(_.at(categoryOptionsAgeGroupsByName, mainAgeGroups)),
    });

    const disaggregatedGroups = _.flatMap(ageGroups, group => {
        const [mainGroupValues, ...restOfAgeGroup] = group;
        if (mainGroupValues.length !== 1) throw "First age group must contain a single element";
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

    return [mainGroup, ...disaggregatedGroups];
}
function getCategoriesMetadataForAntigens(db, sourceData) {
    const ageGroups = _(toKeyList(sourceData, "antigens"))
        .map("ageGroups")
        .flattenDeep()
        .uniq()
        .value();

    const categoryOptionsAgeGroups = sortAgeGroups(ageGroups).map(ageGroup => {
        return db.get("categoryOptions", {
            name: ageGroup,
            shortName: ageGroup,
        });
    });

    const metadata = flattenPayloads(
        toKeyList(sourceData, "antigens").map(antigen => {
            const categoryOptions = db.get("categoryOptions", {
                name: antigen.name,
                code: antigen.code,
                shortName: antigen.name,
            });

            const categoryOptionGroups = getCategoryOptionGroupsForAgeGroups(
                db,
                antigen,
                categoryOptionsAgeGroups
            );

            return { categoryOptions, categoryOptionGroups };
        })
    );

    return flattenPayloads([metadata, { categoryOptions: categoryOptionsAgeGroups }]);
}

function getIndicator(db, indicatorTypesByKey, namespace, plainAttributes) {
    const attributes = interpolateObj(plainAttributes, namespace);

    return db.get("indicators", {
        shortName: attributes.name,
        indicatorType: {
            id: getOrThrow(indicatorTypesByKey, [attributes.$indicatorType, "id"]),
        },
        ...attributes,
    });
}

function getCategoryComboId(db, categoriesMetadata, obj) {
    const categoryCombosByName = _.keyBy(db.getObjectsForModel("categoryCombos"), "name");
    const categoryCombosByKey = _.keyBy(categoriesMetadata.categoryCombos, "key");
    const ccId = obj.$categoryCombo
        ? getOrThrow(categoryCombosByKey, [obj.$categoryCombo, "id"])
        : getOrThrow(categoryCombosByName, ["default", "id"]);
    return { id: ccId };
}

function getCategoryCombosForDataElements(db, dataElement, categoriesMetadata) {
    const categoriesByKey = _.keyBy(categoriesMetadata.categories, "key");
    const categoriesForDataElement = dataElement.$categories;

    if (categoriesForDataElement) {
        return _(categoriesForDataElement)
            .partition("optional")
            .map(categories =>
                categories.map(category => getOrThrow(categoriesByKey, category.key))
            )
            .zip(["Optional", "Required"])
            .flatMap(([categories, typeString]) => {
                return db.get("categoryCombos", {
                    key: `data-element-${dataElement.key}-${typeString}`,
                    code: getCode(["RVC_DE", dataElement.code, typeString]),
                    name: getName(["Data Element", dataElement.name, typeString]),
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

    const groupsByAntigens = _(antigen.dataElements)
        .partition("optional")
        .map(des => des.map(de => getOrThrow(dataElementsByKey, de.key)))
        .zip(["Optional", "Required"])
        .flatMap(([dataElementsForGroup, typeString]) => {
            return db.get("dataElementGroups", {
                key: `data-elements-${antigen.key}-${typeString}`,
                code: getCode([antigen.code, typeString]),
                name: getName(["Antigen", antigen.name, typeString]),
                shortName: getName([antigen.name, "DES", typeString]),
                dataElements: getIds(dataElementsForGroup),
            });
        })
        .value();

    return groupsByAntigens;
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
            const mainGroup = db.get("dataElementGroups", {
                key: "data-elements-antigens",
                code: "RVC_ANTIGEN",
                name: "Antigens",
                shortName: "Antigens",
                dataElements: getIds(dataElementsMetadata.dataElements),
            });

            const dataElementGroupsForAntigen = getDataElementGroupsForAntigen(
                db,
                antigen,
                dataElementsMetadata.dataElements
            );

            const dataElementGroupSetForAntigen = db.get("dataElementGroupSets", {
                key: `data-elements-${antigen.key}`,
                code: `${antigen.code}`,
                dataDimension: false,
                name: getName(["Antigen", antigen.name]),
                dataElementGroups: getIds(dataElementGroupsForAntigen),
            });

            return {
                dataElementGroups: [mainGroup, ...dataElementGroupsForAntigen],
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
    const customMetadata = toKeyList(sourceData, "categories").map(attributes => {
        const $categoryOptions = getOrThrow(attributes, "$categoryOptions");
        const antigenCodes = Object.values(sourceData.antigens).map(antigen => antigen.code);

        const [antigenOptions, ageGroupOptions] = _(categoriesAntigensMetadata.categoryOptions)
            .partition(categoryOption => antigenCodes.includes(categoryOption.code))
            .value();

        let categoryOptions;
        if ($categoryOptions.kind == "fromAntigens") {
            categoryOptions = _.sortBy(antigenOptions, "name");
        } else if ($categoryOptions.kind == "fromAgeGroups") {
            categoryOptions = ageGroupOptions;
        } else if ($categoryOptions.kind == "values") {
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

        return { categories: [category], categoryOptions };
    });

    const payload = flattenPayloads([categoriesAntigensMetadata, ...customMetadata]);
    const categoryByKey = _.keyBy(payload.categories, "key");

    const categoryCombos = _(toKeyList(sourceData, "categoryCombos"))
        .flatMap(categoryCombo => {
            const categoriesForCatCombo = categoryCombo.$categories.map(categoryKey =>
                getOrThrow(categoryByKey, categoryKey)
            );

            return db.get("categoryCombos", {
                dataDimensionType: "DISAGGREGATION",
                categories: getIds(categoriesForCatCombo),
                name: categoriesForCatCombo.map(category => category.name).join(" / "),
                code: categoriesForCatCombo.map(category => category.code).join("_"),
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
        // We may need references to Category Option Combos in datasets->greyfields, so for
        // now we generate them there
        /* const res = await db.updateCOCs();
        if (res.status < 200 || res.status > 299) {
            throw `Error upding category option combo: ${inspect(res)}`;
        } */
    }
    return responseJson;
}

module.exports = { getPayload, postPayload };
