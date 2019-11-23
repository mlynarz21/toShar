# Instruction
1. Initialize project structure in HDFS:
```
./initialize.sh
```

2. Generate list of sources and destination points:
```
./generate_trip_sources_list.sh > sources.txt
```

3. Call acquisition script:
```
./acquisition.sh -i=sources.txt
```
```./acquisition.sh -h``` for details.

4. Check logs stored by default in *acquisition.log*

# Other
Using modification of logging script used from following source: 
https://github.com/adoyle-h/bash-logger
