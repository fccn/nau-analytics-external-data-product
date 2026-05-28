# NAU ANALYTICS - External Entities data product #
## Overview ##
This repository contains all the necessary PySpark scripts, Dockerfile, and SQL queries used to create the data products for the External Entities domain within the NAU platform.

The orchestrator DAGs are maintained in a separate repository due to Kubernetes deployment requirements and organizational structure.

## Structure ## 
 ```
 root/
├────.github/
|     ├── workflows/
|         ├── docker-build-push.yml 
|
├───Docker/
|   ├─── Dockerfile
|
|
├─── src/
|    ├─── bronze/
|         ├── python/
|         |     ├────
|         ├── sql/
|         |
|         ├── utils/
|             ├─── __init__.py
|             ├─── config.py  # dataclass for the necessary config objects 
|             |
|
|
 ```
