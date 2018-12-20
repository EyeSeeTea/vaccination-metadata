const _ = require("lodash");
const fs = require("fs");
const argparse = require("argparse");

const {debug, inspect, interpolate} = require("./utils");
const {Db} = require("./db");

function getIds(objs) {
    return objs.map(obj => ({id: obj.id}));
}

/* Transform an optional nested array of {key1: [...], key2: [...]} into a single accumulated object */
function flattenPayloads(payloads) {
    return _(payloads)
        .map(_.toPairs)
        .flatten()
        .groupBy(([key, _values]) => key)
        .mapValues(pairs => _(pairs).flatMap(([_key, values]) => values).uniqBy("id").value())
        .value();
}

async function getPayload({url, data}) {
    const db = await Db.init(url, {models: [
        "categories",
        "categoryOptions",
        "dataElementGroupSets",
        "dataElementGroups",
        "dataElements",
    ]});

    const antigensCategoryOptions = data.antigens.map(antigen => {
        return db.getByKeyAndName("categoryOptions", {
            name: antigen.name,
            code: antigen.code,
            shortName: antigen.name,
        });
    });

    const payloadForAgeGroups = flattenPayloads(_.flatMap(data.antigens, antigen => {
        const categoryOptions = antigen.ageGroups.map(ageGroupName => {
            return db.getByKeyAndName("categoryOptions", {
                name: ageGroupName,
                shortName: ageGroupName,
            });
        });

        const category = db.getByKeyAndName("categories", {
            key: `age-group-${antigen.key}`,
            name: `Age group ${antigen.name}`,
            dataDimensionType: "DISAGGREGATION",
            dimensionType: "CATEGORY",
            dataDimension: true,
            categoryOptions: getIds(categoryOptions),
        });

        return {categories: [category], categoryOptions};
    }));

    const antigensCategory = db.getByKeyAndName("categories", {
        key: "antigens",
        name: "Antigens",
        dataDimensionType: "DISAGGREGATION",
        dimensionType: "CATEGORY",
        dataDimension: true,
        categoryOptions: getIds(antigensCategoryOptions),
    });

    const vaccinationDataPayload = flattenPayloads(data.vaccinationData.byAntigen.map(group => {
        const dataElements = data.antigens.map(antigen => {
            const groupItemsInterpolated = _(group.items)
                .mapValues(s => interpolate(s, {antigen}))
                .value();

            return db.getByKeyAndName("dataElements", {
                ...groupItemsInterpolated,
                domainType: "AGGREGATE",
                aggregationType: "SUM",
                // categoryCombo: categoryCombosByAntigen[antigen.key] // TODO
            });
        });

        const dataElementGroup = db.getByKeyAndName("dataElementGroups", {
            ...group,
            dataElements: getIds(dataElements),
        });

        return {dataElementGroups: [dataElementGroup], dataElements};
    }));

    const reactiveVaccinationGroupSet = db.getByKeyAndName("dataElementGroupSets", {
        key: "reactive-vaccination",
        name: "Reactive Vaccination",
        dataElementGroups: getIds(vaccinationDataPayload.dataElementGroups),
    });

    const basePayload = {
        categories: [antigensCategory],
        categoryOptions: antigensCategoryOptions,
        ...vaccinationDataPayload,
        dataElementGroupSets: [reactiveVaccinationGroupSet],
    };

    return flattenPayloads([basePayload, payloadForAgeGroups]);
}

async function generate({url, sourceDataFilePath, outputMetadataFilePath}) {
    const sourceData = JSON.parse(fs.readFileSync(sourceDataFilePath, "utf8"));
    const metadata = await getPayload({data: sourceData, url});
    const json = JSON.stringify(metadata, null, 2);
    fs.writeFileSync(outputMetadataFilePath, json, "utf8");
    debug(`Output: ${outputMetadataFilePath}`);
}

async function post({url, sourceMetadataFilePath}) {
    const payload = JSON.parse(fs.readFileSync(sourceMetadataFilePath, "utf8"));
    const db = await Db.init(url);
    const responseJson = await db.postMetadata(payload);
    debug(inspect(responseJson));
}

function getArgsParser() {
    const parser = argparse.ArgumentParser();

    const subparsers = parser.addSubparsers({
        title: "COMMAND",
        dest: "command",
    });

    const parserGenerate = subparsers.addParser("generate", {addHelp: true});
    const parserPost = subparsers.addParser("post", {addHelp: true});
    const urlArg = [["-u", "--url"], {
        required: true,
        help: "DHIS2 instance URL: http://username:password@server:port",
    }];

    parserGenerate.addArgument(...urlArg);
    parserGenerate.addArgument([ "-i", "--data-input" ], {
        dest: "sourceDataFilePath",
        help: "Source JSON data path",
        required: true,
    });
    parserGenerate.addArgument([ "-o", "--metadata-output" ], {
        dest: "outputMetadataFilePath",
        help: "Output JSON metadata path",
        required: true,
    });

    parserPost.addArgument(...urlArg);
    parserPost.addArgument([ "-i", "--metadata-input" ], {
        dest: "sourceMetadataFilePath",
        help: "Source JSON metadata path",
        required: true,
    });

    return parser.parseArgs();
}

function main() {
    const args = getArgsParser();
    
    switch (args.command) {
    case "generate":
        generate(args);
        break;
    case "post":
        post(args);
        break;
    default:
        throw new Error(`Command not implemented: ${args.command}`);
    }
}

main();
