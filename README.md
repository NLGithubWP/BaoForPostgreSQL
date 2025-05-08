![Bao loves PostgreSQL](https://github.com/LearnedSystems/BaoForPostgreSQL/blob/master/branding/bao_loves_pg.svg)

This is a prototype implementation of Bao for PostgreSQL. Bao is a learned query optimizer that learns to "steer" the PostgreSQL optimizer by issuing coarse-grained query hints. For more information about Bao, [check out the paper](https://rm.cab/bao).

Documentation, including a tutorial, is available here: https://rmarcus.info/bao_docs/

While this repository contains working prototype implementations of many of the pieces required to build a production-ready learned query optimizer, this code itself should not be used in production in its current form. Notable limitations include:

* The reward function is currently restricted to being a user-supplied value or the query latency in wall time. Thus, results may be inconsistent with high degrees of parallelism.
* The Bao server component does not perform any level of authentication or encryption. Do not run it on a machine directly accessible from an untrusted network.
* The code has not been audited for security issues. Since the PostgreSQL integration is written using the C hooks system, there are almost certainly issues.

This software is available under the AGPLv3 license. 





### Deploy (origin repo)

Deploy Extension

```bash
# inside docker, go to /code/AI4QueryOptimizer
docker exec -it postgres12 bash
cd /code/AI4QueryOptimizer/pg_extension
make USE_PGXS=1 install
echo "shared_preload_libraries = 'pg_bao'" >> /var/lib/postgresql/data/postgresql.conf

# restart the container
docker restart postgres12
```

Verify extension is installed successfully.

```sql
# inside the docker
docker exec -it postgres12 bash
psql -U postgres
# in psql
SHOW enable_bao;
```

Verify the server is running and can receive requests from the BAO extension.

```sql
# inside the docker
docker exec -it postgres12 bash
psql -U postgres
# run those
\c imdb;
SET enable_bao TO on;
SELECT count(*) FROM users;
EXPLAIN SELECT count(*) FROM title;
```

Dev BAO

If updating the extension C code, do the following.

```bash
cd /code/BaoForPostgreSQL/pg_extension
make USE_PGXS=1 install

# restart the container
docker restart postgres12
docker exec -it  postgres-container bash
export PATH=/usr/lib/postgresql/12/bin:$PATH
# then run the verify above
```

Remove the trained model

```bash
cd /hdd1/xingnaili/AI4QueryOptimizer/baseline/BaoForPostgreSQL/bao_server
rm -rf bao_default_model
```

