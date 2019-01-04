const _ = require("lodash");
const {interpolate, get, cartesianProduct} = require("./utils");
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
                .map(obj => _.omit(obj, ["key"]))
                .value())
        .value();
}

function toKeyList(object) {
    return _(object).map((value, key) => ({...value, key})).value();
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
    const antigensCategoryOptions = toKeyList(sourceData.antigens).map(antigen => {
        return db.getByKeyAndName("categoryOptions", {
            name: antigen.name,
            code: antigen.code,
            shortName: antigen.name,
        });
    });

    const categoryOptionsGender = ["Female", "Male"].map(name => {
        return db.getByKeyAndName("categoryOptions", {name, shortName: name});
    });

    const categoryGender = db.getByKeyAndName("categories", {
        key: "gender",
        name: "Gender",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: false,
        categoryOptions: getIds(categoryOptionsGender),
    });

    const indicatorTypesByKey = _(toKeyList(sourceData.indicatorTypes))
        .map(indicatorType => db.getByKeyAndName("indicatorTypes", indicatorType))
        .keyBy("key")
        .value();

    const getIndicatorType = indicator => ({
        id: get(indicatorTypesByKey, [indicator.indicatorType, "id"]),
    });

    const customCategoriesMetadataByKey = getCustomCategories(sourceData, db);

    const categoriesMetadataByAntigen = getCategoriesMetadataByAntigen(db, sourceData, categoryGender);

    const categoryAntigens = db.getByKeyAndName("categories", {
        key: "antigens",
        name: "Antigens",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: true,
        categoryOptions: getIds(antigensCategoryOptions),
    });

    const categoryCombosByName = _(db.getObjectsForModel("categoryCombos"))
        .keyBy("name")
        .value();

    const dataElementsGlobal = toKeyList(sourceData.vaccinationData.global).map(info => {
        return db.getByKeyAndName("dataElements", {
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

    const dataElementsMetadata = toKeyList(sourceData.vaccinationData.byAntigen).map(group => {
        const dataElements = toKeyList(sourceData.antigens).map(antigen => {
            const groupItemsInterpolated = _(group.items)
                .mapValues(s => interpolate(s, {antigen}))
                .value();

            return db.getByKeyAndName("dataElements", {
                ...groupItemsInterpolated,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                categoryCombo: {id: categoriesMetadataByAntigen[antigen.key].categoryCombos[0].id},
            });
        });

        const dataElementGroup = db.getByKeyAndName("dataElementGroups", {
            ..._(group).omit(["items"]).value(),
            dataElements: getIds(dataElements),
        });

        return {dataElementGroups: [dataElementGroup], dataElements};
    });

    const indicatorsGlobal = toKeyList(sourceData.vaccinationIndicators.global).map(info => {
        return db.getByKeyAndName("indicators", {
            shortName: info.name,
            ...info,
            "indicatorType": getIndicatorType(info),
        });
    });

    const indicatorsMetadataByAntigen =
        getIndicatorsByAntigen(db, sourceData, dataElementsMetadata, dataElementsGlobal, getIndicatorType);

    const reactiveVaccinationDataElementGroupSets = db.getByKeyAndName("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataElementGroups:
            getIds(_(dataElementsMetadata).flatMap(payload => payload.dataElementGroups).value()),
    });

    const reactiveVaccinationIndicatorGroupSet = db.getByKeyAndName("indicatorGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        indicatorGroups:
            getIds(_(indicatorsMetadataByAntigen).flatMap(payload => payload.indicatorGroups).value()),
    });

    const userRoles = toKeyList(sourceData.userRoles).map(userRole => {
        return db.getByKeyAndName("userRoles", userRole);
    });

    const payloadBase = {
        categories: [categoryGender, categoryAntigens],
        categoryOptions: [...categoryOptionsGender, ...antigensCategoryOptions],
        dataElementGroupSets: [reactiveVaccinationDataElementGroupSets],
        indicatorGroupSets: [reactiveVaccinationIndicatorGroupSet],
        indicatorTypes: _.values(indicatorTypesByKey),
        indicators: indicatorsGlobal,
        dataElements: dataElementsGlobal,
        userRoles,
    };

    const payload = flattenPayloads([
        payloadBase,
        ...dataElementsMetadata,
        ...indicatorsMetadataByAntigen,
        ..._.values(customCategoriesMetadataByKey),
        ..._.values(categoriesMetadataByAntigen),
    ]);

    return addCategoryOptionCombos(db, payload);
}

function getCategoriesMetadataByAntigen(db, sourceData, categoryGender) {
    return _(toKeyList(sourceData.antigens)).map(antigen => {
        const categoryOptionsForAge = antigen.ageGroups.map(ageGroupName => {
            return db.getByKeyAndName("categoryOptions", {
                name: ageGroupName,
                shortName: ageGroupName,
            });
        });
        const categoryAgeGroup = db.getByKeyAndName("categories", {
            key: `age-group-${antigen.key}`,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: true,
            categoryOptions: getIds(categoryOptionsForAge),
        });
        const categoryComboAgeGender = db.getByKeyAndName("categoryCombos", {
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

function getIndicatorsByAntigen(db, sourceData, dataElementsMetadata, dataElementsGlobal, getIndicatorType) {
    const dataElementsByKey = _(dataElementsMetadata)
        .flatMap("dataElements")
        .concat(dataElementsGlobal)
        .keyBy("key")
        .value();

    return toKeyList(sourceData.vaccinationIndicators.byAntigen).map(group => {
        const indicators = toKeyList(sourceData.antigens).map(antigen => {
            const groupItemsInterpolated = _(group.items)
                .mapValues(s => interpolate(s, { antigen, dataElements: dataElementsByKey }))
                .value();
            return db.getByKeyAndName("indicators", {
                ...groupItemsInterpolated,
                "indicatorType": getIndicatorType(group.items),
            });
        });
        const indicatorGroup = db.getByKeyAndName("indicatorGroups", {
            ..._(group).omit(["items"]).value(),
            indicators: getIds(indicators),
        });
        return { indicatorGroups: [indicatorGroup], indicators };
    });
}

function getCustomCategories(sourceData, db) {
    const pairs = toKeyList(sourceData.categories).map(info => {
        const categoryOptions = info.categoryOptions.map(name => {
            return db.getByKeyAndName("categoryOptions", {
                name: name,
                shortName: name,
            });
        });
        const category = db.getByKeyAndName("categories", {
            key: info.key,
            name: info.name,
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: false,
            categoryOptions: getIds(categoryOptions),
        });
        const categoryCombo = db.getByKeyAndName("categoryCombos", {
            key: info.key,
            name: info.name,
            dataDimensionType: "DISAGGREGATION",
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