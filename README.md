<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
<a href="http://asterixdb.apache.org"><img src="http://asterixdb.apache.org/img/asterixdb_tm.png" height=100></img></a>
# SmartRabbit: An Interactive Query Processer

The extended paper is present at `SmartRabbit_Extended_Paper.pdf`


## Prerequisites

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.3.9 or newer.
* JDK 11 or newer.
* Python 3.6+ with pip and venv
## Quick Start
1. Build AsterixDB with SmartRabbit (skip tests/RAT/checkstyle):

```
cd asterixdb/
mvn clean package -DskipTests -DskipRat -Dlicense.skip=true -Dcheckstyle.skip=true
```

2. If you load the codebase using an IDE, simply search for AsterixHyracksIntegrationUtil.java and run it. Or you can also do the following :
```
java -cp asterixdb/asterix-server/target/asterix-server-*-binary-assembly.jar \
  org.apache.asterix.api.common.AsterixHyracksIntegrationUtil
```
3. If you want to play with the configs like the buffer cache or number of partitions, you can do it from
```
src/test/resources/cc-main.conf
```
4. For the rest of the python scripts, go to cd asterixdb from the root folder.
5. Use dbgen(https://github.com/electrum/tpch-dbgen) to load TPC-H data. Load TPC-H data from your dbgen output. You can provide the scale factor and the tables created will have the same sf suffix.
```
python3 load_tpch_datasets.py  --sf 10 --base-path /scratch/dbgen-data/SF10
```

6. Run SmartRabbit:
 run_workflow.py can be used to run SmartRabbit.

`run_workflow.py` automates the execution of **SmartRabbit** queries on Apache AsterixDB across multiple **TPC-H scale factors**, **cache clearing**, and **repeated runs** for both **blocking** and **interactive** modes.

It wraps around `SmartRabbitExecutor.py` and uses the AsterixDB REST API to execute queries. The directory that shows the json files with the timings can be set from `SmartRabbitExecutor.py`. You can also see the number of tuples from InteractiveAnswersCount and the Rate of Answers of the Interactive Plan from InteractiveAnswersRate in the same folder.  



### Usage

```
python3 run_workflow.py \
  --sfs 10 30 100 \
  --runs 3 \
  --queries q3 \
  --nodes 204
```
PS: the nodes is just internal bookkeeping I developed to figure out which AsterixDB configuration I was using. There are two optional flags: --interactive-only and --blocking-only which allows you to run only the SmartRabbit run (interactive + blocking) or the pure blocking run respectively.
