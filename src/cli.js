const _ = require("lodash");
const fs = require("fs");
const argparse = require("argparse");
const { debug, inspect, getVersion } = require("./utils");
const { getPayload, postPayload } = require("./metadata");

async function generate({ url, sourceDataFilePath, outputMetadataFilePath }) {
    const version = await getVersion();
    debug(`Source data: ${sourceDataFilePath}`);
    debug(`Version: ${version}`);
    const sourceData = JSON.parse(fs.readFileSync(sourceDataFilePath, "utf8"));
    const metadata = await getPayload(url, sourceData, version);
    const json = JSON.stringify(metadata, null, 2);
    fs.writeFileSync(outputMetadataFilePath, json, "utf8");
    debug(`Metadata output: ${outputMetadataFilePath}`);
}

async function post({ url, sourceMetadataFilePath }) {
    const payloadAll = JSON.parse(fs.readFileSync(sourceMetadataFilePath, "utf8"));
    const payload = _(payloadAll)
        .omit(["organisationUnitLevels", "organisationUnits"])
        .value();

    const responseJson = await postPayload(url, payload, { updateCOCs: true });

    if (responseJson.status === "OK") {
        debug(`Import success: ${inspect(responseJson.stats)}`);
        _(responseJson.typeReports).each(typeReport => {
            const modelName = _.last(typeReport.klass.split("."));
            debug(" - " + `${modelName}: ${inspect(typeReport.stats)}`);
        });
    } else {
        debug("Import error:");
        debug(inspect(responseJson));
    }
}

function getArgsParser() {
    const parser = argparse.ArgumentParser();

    const subparsers = parser.addSubparsers({
        title: "COMMAND",
        dest: "command",
    });

    const parserGenerate = subparsers.addParser("generate", { addHelp: true });
    const parserPost = subparsers.addParser("post", { addHelp: true });
    const urlArg = [
        ["-u", "--url"],
        {
            required: true,
            help: "DHIS2 instance URL: http://username:password@server:port",
        },
    ];

    parserGenerate.addArgument(...urlArg);
    parserGenerate.addArgument(["-i", "--data-input"], {
        dest: "sourceDataFilePath",
        help: "Source JSON data path",
        required: true,
    });
    parserGenerate.addArgument(["-o", "--metadata-output"], {
        dest: "outputMetadataFilePath",
        help: "Output JSON metadata path",
        required: true,
    });

    parserPost.addArgument(...urlArg);
    parserPost.addArgument(["-i", "--metadata-input"], {
        dest: "sourceMetadataFilePath",
        help: "Source JSON metadata path",
        required: true,
    });

    return parser.parseArgs();
}

async function main() {
    const args = getArgsParser();

    switch (args.command) {
        case "generate":
            return generate(args);
        case "post":
            return post(args);
        default:
            throw new Error(`Command not implemented: ${args.command}`);
    }
}

const logErrorAndExit = err => {
    console.error(err); // eslint-disable-line no-console
    process.exit(1);
};

main().catch(logErrorAndExit);
