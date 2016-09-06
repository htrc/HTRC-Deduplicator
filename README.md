# HTRC-Deduplicator
Tool used to deduplicate a set of volumes by matching metadata attributes.

# Build
* To generate a "fat" executable JAR, run:
  `sbt assembly`
  then look for it in `target/scala-2.11/` folder.

  *Note:* you can run the JAR via the usual: `java -jar JARFILE`

* To generate a package that can be invoked via a shell script, run:
  `sbt stage`
  then find the result in `target/universal/stage/` folder.

# Run
```
deduplicator 1.0-SNAPSHOT
HathiTrust Research Center
  -n, --num-partitions  <N>   The number of partitions to split the input data
                              into
  -o, --output  <DIR>         Write the output to DIR
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --help                  Show help message
      --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```
