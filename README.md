# HTRC-Tools-DeduplicateMeta
Given a list of volume IDs, this application loads the metadata information for each volume
(from the associated volume JSON file in the pairtree) and attempts to find "duplicates" by
matching volumes on a subset of the metadata attributes. This subset is currently defined in
the code. A future extension to this tool might allow the user to define this subset externally,
but it's currently not planned. The output from this tool consists of the list of "deduplicated"
IDs.
  
# Build
* To generate an executable package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.

# Run
The following command line arguments are available:
```
deduplicate-meta
HathiTrust Research Center
  -m, --meta                  Flag to specify whether metadata information
                              (title, author, pubDate) should be included in the
                              output
  -n, --num-partitions  <N>   The number of partitions to split the input data
                              into
  -o, --output  <DIR>         Write the output to DIR
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --spark-log  <FILE>     Where to write logging output from Spark to
      --help                  Show help message
      --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```
