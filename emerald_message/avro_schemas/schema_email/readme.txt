AVRO schemas can be packaged in one file or multiple files.  If multiple files are used, they must be read in order.
To accomplish this we we have a set of subfolders where the schemas go.
Atomic files (not built on any of our custom type) go in "level_1.0"
Those depending on level 1.0 go in "level_2.0"

The directory names are not required if there are no dependencies between files - this logic exists only to support
schemas in multiple files.

Pay attention to the schema namespace in each file - a file containing
"namespace": "com.dynastyse.emerald.schemas.email"
is NOT the same as
  "namespace": "com.dynastyse.emerald.schemas"


