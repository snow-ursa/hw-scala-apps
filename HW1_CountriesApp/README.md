## HW1: Scala + SBT + Json

### JAR
Build
`sbt shell > assembly`

Result`target/scala-2.13/HW1_CountriesApp-assembly-0.1.0-SNAPSHOT.jar`

### Usage
```bash
Usage: Parsing application [options]

  -i, --inputJsonPath 
  -o, --outputJsonPath 
  -n, --countryName 
  -c, --countryCount 
```

### Example
```bash
java \
-jar target/scala-2.13/HW1_CountriesApp-assembly-0.1.0-SNAPSHOT.jar \
-i src/main/resources/countries.json \
-o src/main/resources/countries_africa.json \
-n Africa \
-c 10
```
