# Kite Examples Integration Tests

A set of integration tests for the [Kite Examples](https://github.com/kite-sdk/kite-examples).

## Running

First, make sure that the version of the Kite Examples that you have checked out is the
same as this project. (E.g. `master` and `master` to use the latest release,
or `snapshot` and `snapshot` to use a SNAPSHOT version - in which case you will need
to build Kite `snapshot` too and install its artifacts in the local Maven repository.)

Then install the Maven artifacts for the Kite Examples in the local repository by
running the following from the top-level Kite Examples directory:
```bash
for module in $(ls -d -- */); do
  (cd $module; mvn install)
done
```

The integration tests can be run against Hadoop services that are running in the
QuickStart VM, or they can start the necessary services in the same VM. The latter is
the simpler option since you don't have to do any set up (however make sure you don't
have the QuickStart VM running in this case since it can interfere with the tests).
Simply run:

```bash
mvn verify
```

Running against the QuickStart VM is useful for testing the examples against real
services. First, [set up the QuickStart VM](https://github.com/kite-sdk/kite-examples/blob/master/README.md#getting-started)
Then type:

```bash
mvn verify -DuseExternalCluster=true
```

When running against an external cluster, the integration tests assume that your
Hadoop configuration files are in `/etc/hadoop/conf`. If you need to specify a
different directory, use:

```bash
mvn verify -DuseExternalCluster=true -Dhadoop.conf.dir=/my/hadoop/conf
```
