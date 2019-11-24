# Instruction
Script ```run.sh``` performs following tasks: prepare project structure
in HDFS, download required sources (**bike trip** and **weather** data)
from Internet, validate, transform to proper format and place in HDFS.

## Usage
```
./run.sh
```

## Results
To check results after finishing this script, use:
```
./show_structure.sh
```

# Test
Test checking if sample file is processed is stored in ```./test```.
To use it, execute ```./test.sh``` and check logs.
To test script against network interruptions, start test and then manually
disconnect from network.

# Other
Using modification of logging script used from following source: 
https://github.com/adoyle-h/bash-logger
