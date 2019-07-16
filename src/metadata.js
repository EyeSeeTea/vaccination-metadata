const _ = require("lodash");
const { Db } = require("./db");
const { getOrThrow, debug } = require("./utils");
const { flattenPayloads, interpolateObj, getName, getCode } = require("./metadata-utils");
const { toKeyList, getIds, sortAgeGroups } = require("./metadata-utils");

const models = [
    "attributes",

    { name: "organisationUnits", fields: ["id", "name", "level"] },

    { name: "categories", fields: ["id", "name", "categoryOptions[id]"] },
    "categoryCombos",
    "categoryOptions",
    "categoryOptionGroups",
    "legendSets",

    "dataElementGroupSets",
    "dataElementGroups",
    "dataElements",

    "indicatorTypes",
    "indicatorGroupSets",
    "indicatorGroups",
    "indicators",

    "userRoles",
];

async function getPayloadFromDb(db, sourceData, version) {
    const categoryOptionsByKind = getCategoryOptionsByKind(db, sourceData);
    const categoriesMetadata = flattenPayloads([
        categoryOptionsByKind.metadata,
        getCategoriesMetadata(sourceData, db, categoryOptionsByKind),
    ]);

    const dataElementsMetadata = getDataElementsMetadata(db, sourceData, categoriesMetadata);

    const indicatorsMetadata = getIndicatorsMetadata(db, sourceData, dataElementsMetadata);

    const userRoles = toKeyList(sourceData, "userRoles").map(userRole =>
        db.get("userRoles", userRole)
    );

    const legendSets = getLegendSets(db, sourceData);

    const attributes = getAttributes(db, sourceData, version);

    const payloadBase = { userRoles, attributes, legendSets };

    return flattenPayloads([
        payloadBase,
        dataElementsMetadata,
        indicatorsMetadata,
        categoriesMetadata,
    ]);
}

function getAttributes(db, sourceData, version) {
    return toKeyList(sourceData, "attributes").map(attribute => {
        const attributeWithVersion = {
            ...attribute,
            description: `Version: ${version || "unknown"}`,
        };
        return db.get("attributes", attributeWithVersion);
    });
}

function getLegendSets(db, sourceData) {
    return toKeyList(sourceData, "legendSets").map(legendSet => {
        return db.get("legendSets", legendSet);
    });
}

function getCategoryOptionGroupsForAgeGroups(db, antigen, categoryOptionsAgeGroups) {
    const categoryOptionsAgeGroupsByName = _.keyBy(categoryOptionsAgeGroups, "name");
    const ageGroups = getOrThrow(antigen, "ageGroups");
    const mainAgeGroups = ageGroups.map(group => group[0][0]);

    const mainGroup = db.get(
        "categoryOptionGroups",
        {
            name: getName([antigen.name, "Age groups"]),
            code: `${antigen.code}_AGE_GROUP`,
            shortName: getName([antigen.shortName || antigen.name, "AG"]),
            categoryOptions: getIds(_.at(categoryOptionsAgeGroupsByName, mainAgeGroups)),
        },
        { field: "code" }
    );

    const disaggregatedGroups = _.flatMap(ageGroups, group => {
        const [mainGroupValues, ...restOfAgeGroup] = group;
        if (mainGroupValues.length !== 1) throw "First age group must contain a single element";
        const mainGroup = mainGroupValues[0];

        if (_(restOfAgeGroup).isEmpty()) {
            return [];
        } else {
            return restOfAgeGroup.map((options, index) => {
                const sIndex = (index + 1).toString();
                return db.get(
                    "categoryOptionGroups",
                    {
                        name: getName([antigen.name, "Age group", mainGroup, sIndex]),
                        shortName: getName([
                            antigen.shortName || antigen.name,
                            "AGE",
                            mainGroup,
                            sIndex,
                        ]),
                        code: getCode([antigen.code, "AGE_GROUP", mainGroup, sIndex]),
                        categoryOptions: getIds(_.at(categoryOptionsAgeGroupsByName, options)),
                    },
                    { field: "code" }
                );
            });
        }
    });

    return [mainGroup, ...disaggregatedGroups];
}
function getCategoryOptionsByKind(db, sourceData) {
    const ageGroups = _(toKeyList(sourceData, "antigens"))
        .map("ageGroups")
        .flattenDeep()
        .uniq()
        .value();

    const maxDoses = _(toKeyList(sourceData, "antigens"))
        .map("doses")
        .max();

    const categoryOptionsAgeGroups = sortAgeGroups(ageGroups).map(ageGroup => {
        return db.get("categoryOptions", {
            name: ageGroup,
            shortName: ageGroup,
            publicAccess: "rwrw----",
        });
    });

    const categoryOptionsDoses = _.range(1, maxDoses + 1).map(nDose => {
        return db.get("categoryOptions", {
            name: `Dose ${nDose}`,
            publicAccess: "rwrw----",
        });
    });

    const categoryOptionAntigens = toKeyList(sourceData, "antigens").map(antigen => {
        return db.get(
            "categoryOptions",
            {
                key: antigen.key,
                name: antigen.name,
                code: antigen.code,
                shortName: antigen.name,
                publicAccess: "rwrw----",
            },
            {
                field: "code",
            }
        );
    });

    return {
        fromAntigens: categoryOptionAntigens,
        fromAgeGroups: categoryOptionsAgeGroups,
        fromDoses: categoryOptionsDoses,
    };
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
    const categoriesForDataElement = dataElement.$categories || [];

    return _(categoriesForDataElement)
        .partition("optional")
        .map(categories => categories.map(category => getOrThrow(categoriesByKey, category.key)))
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
                name: getName(["RVC", "Antigen", antigen.name, typeString]),
                shortName: getName(["RVC", antigen.name, "DES", typeString]),
                dataElements: getIds(dataElementsForGroup),
            });
        })
        .value();

    return groupsByAntigens;
}

function getDataElementsMetadata(db, sourceData, categoriesMetadata) {
    const dataElementsMetadata = flattenPayloads(
        toKeyList(sourceData, "dataElements").map(dataElement => {
            const formName = _([dataElement.name, dataElement.$order])
                .compact()
                .join(" - ");
            const dataElements = db.get("dataElements", {
                shortName: dataElement.shortName || dataElement.name,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                categoryCombo: getCategoryComboId(db, categoriesMetadata, dataElement),
                ...dataElement,
                formName,
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
                name: "RVC - All Data Elements",
                shortName: "RVC - All Data Elements",
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

function getCategoriesMetadata(sourceData, db, categoryOptionsByKind) {
    const customMetadata = toKeyList(sourceData, "categories").map(attributes => {
        const $categoryOptions = attributes.$categoryOptions;

        let categoryOptions;
        if (!$categoryOptions || !$categoryOptions.kind) {
            categoryOptions = null;
        } else if ($categoryOptions.kind == "values") {
            categoryOptions = $categoryOptions.values.map(name => {
                return db.get("categoryOptions", {
                    name: name,
                    shortName: name,
                    publicAccess: "rwrw----",
                });
            });
        } else {
            categoryOptions = getOrThrow(categoryOptionsByKind, $categoryOptions.kind);
        }

        const category = db.get("categories", {
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: false,
            ...(categoryOptions ? { categoryOptions: getIds(categoryOptions) } : {}),
            ..._.omit(attributes, ["$categoryOptions"]),
        });

        return { categories: [category], categoryOptions: categoryOptions || [] };
    });

    const payload = flattenPayloads(customMetadata);
    const categoryByKey = _.keyBy(payload.categories, "key");
    const { fromAgeGroups, fromDoses } = categoryOptionsByKind;

    const categoryOptionGroupsAgeGroups = _.flatten(
        toKeyList(sourceData, "antigens").map(antigen =>
            getCategoryOptionGroupsForAgeGroups(db, antigen, fromAgeGroups)
        )
    );

    const categoryOptionGroupsDoses = _.flatten(
        toKeyList(sourceData, "antigens").map(antigen => {
            const attributes = {
                name: getName([antigen.name, "Doses"]),
                shortName: getName([antigen.shortName || antigen.name, "Doses"]),
                code: getCode([antigen.code, "DOSES"]),
                categoryOptions: getIds(_.take(fromDoses, antigen.doses)),
            };
            return db.get("categoryOptionGroups", attributes, { field: "code" });
        })
    );

    const categoryCombos = _(toKeyList(sourceData, "categoryCombos"))
        .flatMap(categoryCombo => {
            const categoriesForCatCombo = categoryCombo.$categories.map(categoryKey =>
                getOrThrow(categoryByKey, categoryKey)
            );

            return db.get("categoryCombos", {
                dataDimensionType: "DISAGGREGATION",
                categories: getIds(categoriesForCatCombo),
                name: categoriesForCatCombo
                    .map(category => category.shortName || category.name)
                    .join(" / "),
                code: getCode(categoriesForCatCombo.map(category => category.code)),
                ...categoryCombo,
            });
        })
        .value();

    const categoryOptionGroups = _.concat(categoryOptionGroupsAgeGroups, categoryOptionGroupsDoses);

    return flattenPayloads([payload, { categoryCombos, categoryOptionGroups }]);
}

/* Public interface */

/* Return JSON payload metadata from source data for a specific DHIS2 instance */
async function getPayload(url, sourceData, version) {
    const db = await Db.init(url, { models });
    return getPayloadFromDb(db, sourceData, version);
}

/* POST JSON payload metadata to a DHIS2 instance */
async function postPayload(url, payload, { updateCOCs = false } = {}) {
    const db = await Db.init(url);
    const responseJson = await db.postMetadata(payload);

    if (responseJson.status === "OK" && updateCOCs) {
        debug("Update category option combinations");
        const res = await db.updateCOCs();
        if (res.status < 200 || res.status > 299) {
            console.error(`Error upding category option combo: ${JSON.stringify(res)}`);
        }
    }
    return responseJson;
}

module.exports = { getPayload, postPayload };
