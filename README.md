# beacon4hcnv

## Directory structure
### beacon_test_data folder
- this directory contains CNV data to populate the database once it has been created.
each file contains the data for the corresponding table

### db folder
This folder contains the following files:
- db_schema.sql file that allow user to create an empty beacon4cnv database + functions.
- beacon4hcnv_db.backup to restore at once the structure of the database + the data for the CNV.


## Database code
 * Updating the counts of variants, samples and calls per dataset:
```
UPDATE beacon_dataset_table
SET variant_cnt=count_variants(id),
    call_cnt=count_calls(id),
    sample_cnt=count_samples(id);
```

## Docker setup

Launch the following command on the beacon4hcnv folder

```
pip install docker-compose
docker build -t beacon4hcnv:latest .
docker-compose up -d db
docker-compose up -d beacon
docker-compose ps
docker-compose exec beacon sh
python -m beacon_api


```


