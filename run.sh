#!/bin/bash

MAP1A="127.0.0.1 5001" MAP1B="127.0.0.1 5002" RED1=" 127.0.0.1 5003" PAX1=" 127.0.0.1 5004"
MAP2A="127.0.0.1 5006" MAP2B="127.0.0.1 5007" RED2=" 127.0.0.1 5008" PAX2=" 127.0.0.1 5009"
MAP3A="127.0.0.1 5011" MAP3B="127.0.0.1 5012" RED3=" 127.0.0.1 5013" PAX3=" 127.0.0.1 5014"

# Node 1
/bin/bash -l -c "./mapper.py 1 $MAP1A" &
/bin/bash -l -c "./mapper.py 2 $MAP1B" &
/bin/bash -l -c "./reducer.py $RED1" &
/bin/bash -l -c "./paxosreplicator.py $PAX1 $PAX2 $PAX3" &

# Node 2
/bin/bash -l -c "./mapper.py 1 $MAP2A" &
/bin/bash -l -c "./mapper.py 2 $MAP2B" &
/bin/bash -l -c "./reducer.py $RED2" &
/bin/bash -l -c "./paxosreplicator.py $PAX2 $PAX1 $PAX3" &

# Node 3
/bin/bash -l -c "./mapper.py 1 $MAP3A" &
/bin/bash -l -c "./mapper.py 2 $MAP3B" &
/bin/bash -l -c "./reducer.py $RED3" &
/bin/bash -l -c "./paxosreplicator.py $PAX3 $PAX1 $PAX2" &

wait