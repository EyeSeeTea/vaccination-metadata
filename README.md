Setup metadata to use Vaccination App in a DHIS2 instance.

## Generate metadata

```
$ node src/cli.js generate --url='http://admin:district@localhost:8080' -i source-data.json -o metadata.json
```

This will generate metadata prepared for a specific DHIS2 instance, so objects will be re-used if existing (uses field `name` as key).

## Post metadata

Once the metadata is generated, you can send it the same DHIS2 instance:

```
$ node src/cli.js post --url='http://admin:district@localhost:8080' -i metadata.json
```

Records will be created or updated as necessary. This command may be run many times, no duplicates should be created.