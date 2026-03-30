# ECIFuzz: Fuzzing Distributed Databases via Execution Context Independence

This repository contains the source code, experimental data, and reproduction package for **ECIFuzz**, a context-aware metamorphic fuzzing framework for distributed RDBMSs.

> **Double-Blind Notice**: To comply with the double-blind review process of ASE 2026, all author names, affiliations, and identifying credentials have been anonymized in this repository. 

---

## 1. Code & Environment Setup

This section describes the prerequisites and steps to build and run the ECIFuzz tool.

### Prerequisites
To build and run ECIFuzz, your environment must meet the following requirements:
* **Java**: Version 11 or higher.
* **Maven**: For dependency management and building the Java-based fuzzer core.
* **Go**: Version 1.22 or higher (needed for TiDB test script).

### Building the Project
1. Clone or download this repository.
2. Navigate to the project root directory.
3. Build the core SQLancer-based engine using Maven:
   ```bash
   mvn clean package -DskipTests

### Test the DBMS

First, you need to setup the DBMS to be tested, and make sure it can be accessed via host and password. 

Then, in the directiory /ECIFuzz: 

For TiDB:

```
java -jar /ECIFuzz/target/sqlancer-*.jar  --host=YOUR_HOST --username=YOUR_USER_NAME --password="YOUR_PASSWORD" --num-tries 100000 --num-queries 30 --num-threads 1  tidb --oracle DIST --queries-per-batch 5 --use-seed-pool &
```

For MySQL NDB Cluster:

```
java -jar /ECIFuzz/target/sqlancer-*.jar --host=YOUR_HOST --username=YOUR_USER_NAME --password="YOUR_PASSWORD" --num-tries 100000 --num-queries 30 --num-threads 1 mysql --oracle DIST --enable-mutate  &
```

For CockroachDB: 

```
java -jar /ECIFuzz/target/sqlancer-*.jar --host=YOUR_HOST --username=YOUR_USER_NAME --password="YOUR_PASSWORD" --port=26258 --num-tries 100000 --num-queries 30 cockroachdb --oracle DIST --enable-mutate &
```

## 2. Evaluation

### Detailed Bug List

ECIFuzz has detected **35 unique bugs**across three distributed RDBMSs. The complete list of identified bugs, their types, severity levels, and current statuses are detailed in the table below.

| DBMS            | Bug ID (Issue Link)                                          | Type  | Severity | Status     |
| :-------------- | :----------------------------------------------------------- | :---- | :------- | :--------- |
| **TiDB**        | [#53088](https://github.com/pingcap/tidb/issues/53088)       | Logic | Major    | Fixed      |
| TiDB            | [#58064](https://github.com/pingcap/tidb/issues/58064)       | Logic | Major    | Fixed      |
| TiDB            | [#57848](https://github.com/pingcap/tidb/issues/57848)       | Logic | Major    | Submitted  |
| TiDB            | [#57861](https://github.com/pingcap/tidb/issues/57861)       | Logic | Major    | Submitted  |
| TiDB            | [#57862](https://github.com/pingcap/tidb/issues/57862)       | Logic | Major    | Submitted  |
| TiDB            | [#65662](https://github.com/pingcap/tidb/issues/65662)       | Logic | Major    | Submitted  |
| TiDB            | [#53900](https://github.com/pingcap/tidb/issues/53900)       | Logic | Moderate | Confirmed  |
| TiDB            | [#65981](https://github.com/pingcap/tidb/issues/65981)       | Logic | Moderate | Submitted  |
| TiDB            | [#53864](https://github.com/pingcap/tidb/issues/53864)       | Logic | Minor    | Confirmed  |
| TiDB            | [#53365](https://github.com/pingcap/tidb/issues/53365)       | Logic | Minor    | Confirmed  |
| TiDB            | [#58025](https://github.com/pingcap/tidb/issues/58025)       | Logic | Minor    | Confirmed  |
| TiDB            | [#58022](https://github.com/pingcap/tidb/issues/58022)       | Logic | Minor    | Submitted  |
| TiDB            | [#53766](https://github.com/pingcap/tidb/issues/53766)       | Crash | Major    | Fixed      |
| TiDB            | [#53865](https://github.com/pingcap/tidb/issues/53865)       | Crash | Major    | Fixed      |
| TiDB            | [#65660](https://github.com/pingcap/tidb/issues/65660)       | Crash | Major    | Fixed      |
| TiDB            | [#55705](https://github.com/pingcap/tidb/issues/55705)       | Crash | Major    | Fixed      |
| TiDB            | [#53290](https://github.com/pingcap/tidb/issues/53290)       | Crash | Major    | Confirmed  |
| TiDB            | [#55397](https://github.com/pingcap/tidb/issues/55397)       | Crash | Major    | Submitted  |
| TiDB            | [#55483](https://github.com/pingcap/tidb/issues/55483)       | Crash | Moderate | Fixed      |
| TiDB            | [#55438](https://github.com/pingcap/tidb/issues/55438)       | Crash | Moderate | Fixed      |
| TiDB            | [#53692](https://github.com/pingcap/tidb/issues/53692)       | Crash | Moderate | Confirmed  |
| TiDB            | [#55599](https://github.com/pingcap/tidb/issues/55599)       | Crash | Moderate | Confirmed  |
| TiDB            | [#65661](https://github.com/pingcap/tidb/issues/65661)       | Crash | Minor    | Submitted  |
| TiDB            | [#57860](https://github.com/pingcap/tidb/issues/57860)       | Crash | --       | Submitted  |
| TiDB            | [#57439](https://github.com/pingcap/tidb/issues/57439)       | Crash | --       | Submitted  |
| TiDB            | [#55344](https://github.com/pingcap/tidb/issues/55344)       | Hang  | Moderate | Fixed      |
| **MySQL NDB**   | [#117476](https://bugs.mysql.com/bug.php?id=117476)          | Logic | S3       | Confirmed  |
| MySQL NDB       | [#117716](https://bugs.mysql.com/bug.php?id=117716)          | Logic | S3       | Fixed      |
| MySQL NDB       | [#119665](https://bugs.mysql.com/bug.php?id=119665)          | Logic | S3       | Submitted  |
| MySQL NDB       | [#119838](https://bugs.mysql.com/bug.php?id=119838)          | Logic | S3       | Submitted  |
| MySQL NDB       | [#119663](https://bugs.mysql.com/bug.php?id=119663)          | Crash | S3       | Confirmed  |
| MySQL NDB       | [#119868](https://bugs.mysql.com/bug.php?id=119868)          | Crash | S3       | Confirmed  |
| MySQL NDB       | [#117303](https://bugs.mysql.com/bug.php?id=117303)          | Crash | S3       | Duplicated |
| **CockroachDB** | [#166385](https://github.com/cockroachdb/cockroach/issues/166385) | Logic | --       | Submitted  |
| CockroachDB     | [#149605](https://github.com/cockroachdb/cockroach/issues/149605) | Crash | --       | Duplicated |
| CockroachDB     | [#160916](https://github.com/cockroachdb/cockroach/issues/160916) | Crash | --       | Duplicated |
| CockroachDB     | [#149808](https://github.com/cockroachdb/cockroach/issues/149808) | Crash | --       | Duplicated |
| CockroachDB     | [#164205](https://github.com/cockroachdb/cockroach/issues/164205) | Crash | --       | Duplicated |
| CockroachDB     | [#164669](https://github.com/cockroachdb/cockroach/issues/164669) | Crash | --       | Submitted  |
| CockroachDB     | [#164715](https://github.com/cockroachdb/cockroach/issues/164715) | Crash | --       | Submitted  |

## 3. Baseline Comparison & Ablation Study Data

This section provides the raw issue links and logs supporting our comparative analysis and ablation studies as presented in the Evaluation section of the paper.

### 3.1 Baseline Comparison
We compare **ECIFuzz** against state-of-the-art database testing tools: **SQLancer** and **Radar**. Below are the issues successfully detected by each tool during our evaluation campaigns.

#### SQLancer
* **MySQL NDB Cluster**:
  * [#119721](https://bugs.mysql.com/bug.php?id=119721) 
  * [#119722](https://bugs.mysql.com/bug.php?id=119722) 
  * [#119723](https://bugs.mysql.com/bug.php?id=119723) 
* **TiDB**:
  * [#65649 (Cast error)](https://github.com/pingcap/tidb/issues/65649)
  * [#65650 (Predicate pushdown)](https://github.com/pingcap/tidb/issues/65650)
  * [#65651 (Right join bug)](https://github.com/pingcap/tidb/issues/65651)
  * [#65653 (Unstable result)](https://github.com/pingcap/tidb/issues/65653)
  * [#65654 (Precision issue)](https://github.com/pingcap/tidb/issues/65654)
* **CockroachDB**:
  * [#160916](https://github.com/cockroachdb/cockroach/issues/160916)

#### Radar
* **TiDB**: [#61949](https://github.com/pingcap/tidb/issues/61949), [#61948](https://github.com/pingcap/tidb/issues/61948), [#61946](https://github.com/pingcap/tidb/issues/61946), [#61947](https://github.com/pingcap/tidb/issues/61947)
* **MySQL NDB Cluster**: [#118551](https://bugs.mysql.com/bug.php?id=118551), [#118552](https://bugs.mysql.com/bug.php?id=118552), [#118553](https://bugs.mysql.com/bug.php?id=118553), [#118555](https://bugs.mysql.com/bug.php?id=118555), [#118556](https://bugs.mysql.com/bug.php?id=118556)

---

### 3.2 Ablation Study
To evaluate the contribution of each component in ECIFuzz, we conducted ablation studies across different configurations.

#### MySQL NDB Cluster Configurations
* **No Tailored Table Generation and Data Population (`mysql_no_env`)**:
  * [#119663](https://bugs.mysql.com/bug.php?id=119663) 
  * [#119665](https://bugs.mysql.com/bug.php?id=119665) 
* **No Mutation (`mysql_no_mutation`)**: 
  * [#119663](https://bugs.mysql.com/bug.php?id=119663)
* **Full Framework (`mysql_full`)**: 
  * [#119663](https://bugs.mysql.com/bug.php?id=119663), [#119665](https://bugs.mysql.com/bug.php?id=119665), [#119838](https://bugs.mysql.com/bug.php?id=119838) 

#### TiDB Configurations
* **No Tailored Table Generation and Data Population (`tidb_no_env`)**: 
  * [#53290](https://github.com/pingcap/tidb/issues/53290)
  * [#65981](https://github.com/pingcap/tidb/issues/65981) (New)
* **No Mutation (`tidb_no_mutation`)**: 
  * [#53290](https://github.com/pingcap/tidb/issues/53290)
  * [#65660](https://github.com/pingcap/tidb/issues/65660)
* **Full Framework (`tidb_full`)**: 
  * [#53290](https://github.com/pingcap/tidb/issues/53290)
  * [#65660](https://github.com/pingcap/tidb/issues/65660)
  * [#65661](https://github.com/pingcap/tidb/issues/65661) 
  * [#65662](https://github.com/pingcap/tidb/issues/65662)

#### CockroachDB Configurations
* **No Tailored Table Generation and Data Population (`cockroachdb_no_env`)**: 
  * [#160916](https://github.com/cockroachdb/cockroach/issues/160916)
  * [#149808](https://github.com/cockroachdb/cockroach/issues/149808)
* **No Mutation (`cockroachdb_no_mutation`)**: 
  * [#164715](https://github.com/cockroachdb/cockroach/issues/164715)
  * [#160916](https://github.com/cockroachdb/cockroach/issues/160916)
* **Full Framework (`cockroachdb_full`)**: 
  * [#160916](https://github.com/cockroachdb/cockroach/issues/160916)
  * [#149808](https://github.com/cockroachdb/cockroach/issues/149808)
  * [#164715](https://github.com/cockroachdb/cockroach/issues/164715)
  * [#166385](https://github.com/cockroachdb/cockroach/issues/166385)