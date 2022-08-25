# iceberg-table-generator

## Usage

```shell
./gradlew run --args="--warehouse <warehouse-path> [--conf <hadoop-conf-key>=<value> ]"
```
Default warehouse path is `$HOME/warehouse`.

### GCP

Path to your JSON keyfile needs to be provided:

```shell
./gradlew run --args="--warehouse gs://path/to/warehouse --conf google.cloud.auth.service.account.json.keyfile=/path/to/keyfile.json"
```
