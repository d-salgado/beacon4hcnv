"""
Genomic SNP endpoint

This endpoint is specific for querying all variants that appear in a certain region, hence the 
parameters accepted by the request differ from the ones in the basic query endpoint.
"""

import ast
import logging
import requests

from .exceptions import BeaconBadRequest, BeaconServerError, BeaconForbidden, BeaconUnauthorised
from .. import __apiVersion__
from ..conf.config import DB_SCHEMA

from ..utils.polyvalent_functions import create_prepstmt_variables, filter_exists, datasetHandover
from ..utils.polyvalent_functions import prepare_filter_parameter, parse_filters_request
from ..utils.polyvalent_functions import fetch_datasets_access, access_resolution

from ..utils.polyvalent_functions import filter_response
from .access_levels import ACCESS_LEVELS_DICT
from ..utils.translate2accesslevels import region2access

LOG = logging.getLogger(__name__)



# ----------------------------------------------------------------------------------------------------------------------
#                                         HANDOVER and extra ANNOTATION
# ----------------------------------------------------------------------------------------------------------------------

def snp_resultsHandover(variantId):
    """Create the resultsHanover dict by inserting the variantId into the template."""

    resultsHandover = [ {
                        "handoverType" : {
                        "id" : "data:1106",
                        "label" : "dbSNP ID"
                        },
                        "note" : "Link to dbSNP database",
                        "url" : f"https://www.ncbi.nlm.nih.gov/snp/?term={variantId}"
                        }, {
                        "handoverType" : {
                        "id" : "data:1106",
                        "label" : "dbSNP ID"
                        },
                        "note" : "Link to dbSNP API",
                        "url" : f"https://api.ncbi.nlm.nih.gov/variation/v0/beta/refsnp/{variantId[2:]}"
                        } ]

    return resultsHandover

async def variantAnnotations(variant_details):
    """
    Create the variantAnnotations response by fetching the cellBase API and the dbSNP API.
    The variant_id has to be in the following format: chrom:start:ref:alt. 
    If in the variantDetails the alt is null, it has to be changed to a '-'.
    """

    variantAnnotations = {}
    
    # cellBase
    chrom = variant_details.get("chromosome")
    start = variant_details.get("start")
    ref = variant_details.get("referenceBases")
    alt = variant_details.get("alternateBases") if variant_details.get("alternateBases") else '-'

    variant_id = ":".join([str(chrom), str(start + 1), ref, alt])
    url = f"http://cellbase.clinbioinfosspa.es/cb/webservices/rest/v4/hsapiens/genomic/variant/{variant_id}/annotation"
    r = requests.get(url)
    cellBase_dict = r.json()
    try:
        cellBase_rsID = cellBase_dict["response"][0]["result"][0]["id"]
    except:
        cellBase_rsID = None

    # dbSNP
    rsID = variant_details.get("variantId") if variant_details.get("variantId") != "." else cellBase_rsID
    if rsID:
        url = f"https://api.ncbi.nlm.nih.gov/variation/v0/beta/refsnp/{rsID[2:]}"
        r = requests.get(url)
        dnSNP_dict = r.json()
    else:
        dnSNP_dict = ''

    return rsID, cellBase_dict, dnSNP_dict


# ----------------------------------------------------------------------------------------------------------------------
#                                         FORMATTING
# ----------------------------------------------------------------------------------------------------------------------


async def transform_record(db_pool, record):
    """Format the record we got from the database to adhere to the response schema."""

    # Before creating the final dict, we want to get the stable_id of the dataset from the DB
    async with db_pool.acquire(timeout=180) as connection:
        try: 
            query = f"""SELECT stable_id, access_type
                        FROM beacon_dataset
                        WHERE id={dict(record).get("dataset_id")};
                        """
            statement = await connection.prepare(query)
            extra_record = await statement.fetchrow()
        except Exception as e:
            raise BeaconServerError(f'Query metadata (stableID) DB error: {e}') 

    response = dict(record)

    # Remove the dispensable parameters of the response dict if you want to return it directly  
    # for dispensable in ["data_id", "dataset_id", "reference_genome", "chromosome", "rs_id", "reference", "type"]:
    #     response.pop(dispensable)

    # Create a final_response dict with the parameters you want to show
    final_response = {}
    dataset_name = dict(extra_record).pop("stable_id")   
    final_response["datasetId"] = dataset_name
    final_response["internalId"] = response.pop("dataset_id")
    final_response["exists"] = True
    final_response["variantCount"] = '' if response.get("variant_cnt") is None else response.pop("variant_cnt")  
    final_response["callCount"] = '' if response.get("call_cnt") is None else response.pop("call_cnt") 
    final_response["sampleCount"] = '' if response.get("sample_cnt") is None else response.pop("sample_cnt") 
    final_response["frequency"] = 0 if response.get("frequency") is None else float(round(response.pop("frequency"), 4))
    final_response["numVariants"] = 0 if response.get("num_variants") is None else response.pop("num_variants")
    final_response["info"] = {"accessType": dict(extra_record).pop("access_type"),
                              "matchingSampleCount": 0 if response.get("matching_sample_cnt") is None else response.pop("matching_sample_cnt"),
                              "cnvInfo": {
                                "svLength": response.get("sv_length"),
                                "genotype": response.get("genotype"),
                                "copyNumberLevel": response.get("copyNumberLevel"),
                                "readDepth": response.get("read_depth"),
                                "genotypeLikelihood": response.get("genotype_likelihood"),
                                "extraInfo": response.get("extra_info"),
                                },
                              "sampleInfo": {
                                "sampleID": response.get("sample_id"),
                                "tissue": response.get("tissue"),
                                "sex": response.get("sex"),
                                "age": response.get("age"),
                                "disease": response.get("disease"),
                                "sampleDescription": response.get("sample_description")
                                }
                              }
    final_response["datasetHandover"] = datasetHandover(dataset_name)
    
    return final_response


def transform_misses(record):
    """Format the missed datasets record we got from the database to adhere to the response schema."""
    response = {}

    dataset_name = dict(record).get("stableId") 

    response["datasetId"] = dataset_name 
    response["internalId"] = dict(record).get("datasetId")
    response["exists"] = False
    # response["datasetId"] = ''  
    response["variantCount"] = 0
    response["callCount"] = 0
    response["sampleCount"] = 0
    response["frequency"] = 0 
    response["numVariants"] = 0 
    response["info"] = {"accessType": dict(record).get("accessType"),
                        "matchingSampleCount": 0 }
    response["datasetHandover"] = datasetHandover(dataset_name)
    return response



# ----------------------------------------------------------------------------------------------------------------------
#                                         MAIN QUERY TO THE DATABASE
# ----------------------------------------------------------------------------------------------------------------------

async def fetch_resulting_datasets(db_pool, processed_request, misses=False, accessible_missing=None, valid_datasets=None):
    """Find datasets based on filter parameters.
    """
    print("Inside fetch_resulting_datasets\n")
    async with db_pool.acquire(timeout=180) as connection:
        datasets = []
        try: 
            if misses:
                if accessible_missing:
                    query = f"""SELECT id as "datasetId", access_type as "accessType", stable_id as "stableId"
                                FROM beacon_dataset
                                WHERE id IN ({create_prepstmt_variables(len(accessible_missing))});
                                """
                    LOG.debug(f"QUERY to fetch accessible missing info: {query}")
                    statement = await connection.prepare(query)
                    db_response =  await statement.fetch(*accessible_missing)
                else:
                    return []
            else:
                print("\t else")
                # Gathering the variant related parameters passed in the request
                assembly_id = '' if not processed_request.get("assemblyId") else processed_request.get("assemblyId")
                chromosome = '' if not processed_request.get("referenceName") else processed_request.get("referenceName") 
                reference = '' if not processed_request.get("referenceBases") else processed_request.get("referenceBases") 
                alternate = '' if not processed_request.get("alternateBases") else processed_request.get("alternateBases")
                genotype = '' if not processed_request.get("genotype") else processed_request.get("genotype")

                start = 'null' if not processed_request.get("start") else processed_request.get("start")
                end = 'null' if not processed_request.get("end") else processed_request.get("end")

                copy_number_level = 'null' if not processed_request.get("copyNumberLevel") else processed_request.get("copyNumberLevel")
                cnv_length = 'null' if not processed_request.get("cnvLength") else processed_request.get("cnvLength")
                max_length = 'null' if not processed_request.get("maxLength") else processed_request.get("maxLength")
                min_length = 'null' if not processed_request.get("minLength") else processed_request.get("minLength")

                # valid_datasets = ",".join([str(i) for i in valid_datasets])

                query = f"""SELECT * FROM beacon_all_data_view
                            WHERE dataset_id IN ({valid_datasets}) 
                            AND reference_genome = '{assembly_id}'
                            AND reference = '{reference}'
                            AND chromosome = '{chromosome}' 
                            AND start >= '{start}'
                            AND "end" <= '{end}'
                            AND (CASE
                                WHEN nullif('{alternate}', '') IS NOT NULL THEN alternate = '{alternate}' ELSE true
                                END)
                            AND (CASE
                                WHEN nullif('{genotype}', '') IS NOT NULL THEN genotype = '{genotype}' ELSE true
                                END)
                            AND (CASE
                                WHEN {copy_number_level} IS NOT NULL THEN copy_number_level = {copy_number_level} ELSE true
                                END)    
                            AND (CASE
                                WHEN {cnv_length} IS NOT NULL THEN sv_length = {cnv_length} ELSE true
                                END)
                            AND (CASE
                                WHEN {max_length} IS NOT NULL THEN sv_length <= {max_length} ELSE true
                                END)
                            AND (CASE
                                WHEN {min_length} IS NOT NULL THEN sv_length >= {min_length} ELSE true
                                END);"""

                LOG.debug(f"QUERY to fetch hits: {query}")
                statement = await connection.prepare(query)
                db_response = await statement.fetch()         

            for record in list(db_response):
                processed = transform_misses(record) if misses else record
                datasets.append(processed)
            return datasets
        except Exception as e:
                raise BeaconServerError(f'Query resulting datasets DB error: {e}') 
    

async def get_datasets(db_pool, query_parameters, include_dataset, processed_request):
    """Find datasets based on filter parameters.
    """
    all_datasets = []
    dataset_ids = query_parameters[-2]
    print("Inside get_datasets\n")
    # Fetch the records of all the hit datasets
    all_datasets = await fetch_resulting_datasets(db_pool, processed_request, valid_datasets=dataset_ids)
    # Then parse the records to be able to separate them by variants, note that we add the hit records already transformed to form the datasetAlleleResponses
    variants_dict = {}
    for record in all_datasets:
        #important_parameters = map(str, [record.get("chromosome"), record.get("variant_id"), record.get("reference"), record.get("alternate"), record.get("start"), record.get("end"), record.get("variant_type")])
        #variant_identifier = "|".join(important_parameters)
        variant_identifier = record.get("data_id")

        if variant_identifier not in variants_dict.keys():
            variants_dict[variant_identifier] = {}
            variants_dict[variant_identifier]["variantDetails"] = {
                "variantId": record.get("variant_id"),
                "chromosome":  record.get("chromosome"),
                "referenceBases": record.get("reference"),
                "alternateBases": record.get("alternate"),
                "variantType": record.get("variant_type"),
                "start": record.get("start"), 
                "end": record.get("end")
            }
            variants_dict[variant_identifier]["datasetAlleleResponses"] = []
            variants_dict[variant_identifier]["datasetAlleleResponses"].append(await transform_record(db_pool, record))
        else:
            variants_dict[variant_identifier]["datasetAlleleResponses"].append(await transform_record(db_pool, record))

    # If  the includeDatasets option is ALL or MISS we have to "create" the miss datasets (which will be tranformed also) and join them to the datasetAlleleResponses
    if include_dataset in ['ALL', 'MISS']:
        for variant in variants_dict:
            list_hits = [record["internalId"] for record in variants_dict[variant]["datasetAlleleResponses"]]
            list_all = list(map(int, dataset_ids.split(",")))
            accessible_missing = [int(x) for x in list_all if x not in list_hits]
            miss_datasets = await fetch_resulting_datasets(db_pool, processed_request, misses=True, accessible_missing=accessible_missing)
            variants_dict[variant]["datasetAlleleResponses"] += miss_datasets

    # Finally, we iterate the variants_dict to create the response
    response = []
    for variant in variants_dict:
        rsID, cellBase_dict, dbSNP_dict = await variantAnnotations(variants_dict[variant]["variantDetails"])
        if rsID: variants_dict[variant]["variantDetails"]["variantId"] = rsID
        datasetAlleleResponses = filter_exists(include_dataset, variants_dict[variant]["datasetAlleleResponses"])
        final_variantsFound_element = {
            "variantDetails": variants_dict[variant]["variantDetails"],
            "datasetAlleleResponses": datasetAlleleResponses,
            "variantAnnotations": {
                "cellBase": cellBase_dict,
                "dbSNP": dbSNP_dict
            },
            "variantHandover": snp_resultsHandover(rsID) if rsID else '',
            "info": {}
        }

        response.append(final_variantsFound_element)

    return response
    

# ----------------------------------------------------------------------------------------------------------------------
#                                         HANDLER FUNCTION
# ----------------------------------------------------------------------------------------------------------------------

async def cnv_request_handler(db_pool, processed_request, request):
    """
    Execute query with SQL funciton.
    """
    # First we parse the query to prepare it to be used in the SQL function
    # We create a list of the parameters that the SQL function needs
    correct_parameters =  [
	"variantType",
	"start",
	"startMin",
	"startMax",
	"end",
	"endMin",
	"endMax",
	"referenceName",
	"referenceBases",
	"alternateBases",
	"assemblyId",
	"datasetIds",
    "filters"]
    
    int_params = ['start', 'end', 'endMax', 'endMin', 'startMax', 'startMin']

    query_parameters = []

    # Iterate correct_parameters to create the query_parameters list from the processed_request 
    # in the requiered order and with the right types
    for param in correct_parameters:
        query_param = processed_request.get(param)
        if query_param:
            if param in int_params:
                query_parameters.append(int(query_param))
            else:
                query_parameters.append(str(query_param))
        else:
            if param in int_params:
                query_parameters.append(None)
            else:
                query_parameters.append("null")


    # At this point we have a list with the needed parameters called query_parameters, the only thing 
    # laking is to update the datasetsIds (it can be "null" or processed_request.get("datasetIds"))

    # Not for CNV
    # LOG.debug(f"Correct param: {correct_parameters}")
    # LOG.debug(f"Query param: {query_parameters}")
    # LOG.debug(f"Query param types: {[type(x) for x in query_parameters]}")

    # We want to get a list of the datasets available in the database separated in three lists
    # depending on the access level (we check all of them if the user hasn't specified anything, if some
    # there were given, those are the only ones that are checked)
    public_datasets, registered_datasets, controlled_datasets = await fetch_datasets_access(db_pool, query_parameters[-2])

    ##### TEST
    # access_type, accessible_datasets = access_resolution(request, request['token'], request.host, public_datasets,
    #                                                      registered_datasets, controlled_datasets)
    # LOG.info(f"The user has this types of acces: {access_type}")
    # query_parameters[-2] = ",".join([str(id) for id in accessible_datasets])
    ##### END TEST

    # NOTICE that rigth now we will just focus on the PUBLIC ones to easen the process, so we get all their 
    # ids and add them to the query
    query_parameters[-2] = ",".join([str(id) for id in public_datasets])

    # We adapt the filters parameter to be able to use it in the SQL function (e.g. '(technology)::jsonb ?& array[''Illumina Genome Analyzer II'', ''Illumina HiSeq 2000'']')
    if query_parameters[-1] != "null":
        processed_filters_param = await prepare_filter_parameter(db_pool, query_parameters[-1])
        query_parameters[-1]  = processed_filters_param

    # We will output the datasets depending on the includeDatasetResponses parameter
    include_dataset = ""
    if processed_request.get("includeDatasetResponses"):
        include_dataset  = processed_request.get("includeDatasetResponses")
    else:
        include_dataset  = "ALL"

    # Not for CNV
    # LOG.info(f"Query FINAL param: {query_parameters}")

    LOG.info('Connecting to the DB to make the query.')
    variantsFound = await get_datasets(db_pool, query_parameters, include_dataset, processed_request)
    LOG.info('Query done.')

    # Generate the variantsFound response


    # We create the final dictionary with all the info we want to return
    beacon_response = { 'beaconId': '.'.join(reversed(request.host.split('.'))),
                        'apiVersion': __apiVersion__,
                        'exists': any([dataset['exists'] for variant in variantsFound for dataset in variant["datasetAlleleResponses"]]),
                        # Error is not required and should not be shown unless exists is null
                        # If error key is set to null it will still not validate as it has a required key errorCode
                        # Setting this will make schema validation fail
                        # "error": None,
                        'request': processed_request,
                        'variantsFound': variantsFound,
                        'info': None,
                        'resultsHandover': None,
                        'beaconHandover': [ { "handoverType" : {
                                                "id" : "CUSTOM",
                                                "label" : "Organization contact"
                                                },
                                                "note" : "Organization contact details maintaining this Beacon",
                                                "url" : "mailto:beacon.ega@crg.eu"
                                            } ]
                        
                        }
    
    # Before returning the response we need to filter it depending on the access levels
    beacon_response = {"beconGenomicRegionRequest": beacon_response}  # Make sure the key matches the name in the access levels dict
    # accessible_datasets = ["EGAD00001000740", "EGAD00001000741"]  # NOTE we use the public_datasets because authentication is not implemented yet
    accessible_datasets = ["BO_SAMPLE"]
    user_levels = ["PUBLIC"]  # NOTE we hardcode it because authentication is not implemented yet
    filtered_response = filter_response(beacon_response, ACCESS_LEVELS_DICT, accessible_datasets, user_levels, region2access)

    return filtered_response["beconGenomicRegionRequest"]