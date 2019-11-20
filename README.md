# beacon4hcnv

## Database code
 * Updating the counts of variants, samples and calls per dataset:
```
UPDATE beacon_dataset_table
SET variant_cnt=count_variants(id),
    call_cnt=count_calls(id),
    sample_cnt=count_samples(id);
```
