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

    # Before creating the dict, we want to get the stable_id frm the DB
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

    for dispensable in ["id", "variant_id", "variant_composite_id", "chromosome", "reference", "alternate", "start", "end", "variant_type"]:
        response.pop(dispensable)

    dataset_name = dict(extra_record).pop("stable_id")   
    response["datasetId"] = dataset_name
    response["internalId"] = response.pop("dataset_id")
    response["exists"] = True
    response["variantCount"] = response.pop("variant_cnt")  
    response["callCount"] = response.pop("call_cnt") 
    response["sampleCount"] = response.pop("sample_cnt") 
    response["frequency"] = 0 if response.get("frequency") is None else float(round(response.pop("frequency"), 4))
    response["numVariants"] = 0 if response.get("num_variants") is None else response.pop("num_variants")
    response["info"] = {"accessType": dict(extra_record).pop("access_type"),
                        "matchingSampleCount": 0 if response.get("matching_sample_cnt") is None else response.pop("matching_sample_cnt")}
    response["datasetHandover"] = datasetHandover(dataset_name)
    
    return response


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

async def fetch_resulting_datasets(db_pool, query_parameters, misses=False, accessible_missing=None):
    """Find datasets based on filter parameters.
    """
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
                query = f"""SELECT * FROM {DB_SCHEMA}.query_data_response({create_prepstmt_variables(13)});"""
                LOG.debug(f"QUERY to fetch hits: {query}")
                statement = await connection.prepare(query)
                db_response = await statement.fetch(*query_parameters)         

            for record in list(db_response):
                processed = transform_misses(record) if misses else record
                datasets.append(processed)
            return datasets
        except Exception as e:
                raise BeaconServerError(f'Query resulting datasets DB error: {e}') 
    

async def get_datasets(db_pool, query_parameters, include_dataset):
    """Find datasets based on filter parameters.
    """
    all_datasets = []
    dataset_ids = query_parameters[-2]

    # Fetch the records of all the hit datasets
    all_datasets = await fetch_resulting_datasets(db_pool, query_parameters)

    # Then parse the records to be able to separate them by variants, note that we add the hit records already transformed to form the datasetAlleleResponses
    variants_dict = {}
    for record in all_datasets:
        #important_parameters = map(str, [record.get("chromosome"), record.get("variant_id"), record.get("reference"), record.get("alternate"), record.get("start"), record.get("end"), record.get("variant_type")])
        #variant_identifier = "|".join(important_parameters)
        variant_identifier = record.get("variant_composite_id")

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
            miss_datasets = await fetch_resulting_datasets(db_pool, query_parameters, misses=True, accessible_missing=accessible_missing)
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

async def region_request_handler(db_pool, processed_request, request):
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

    LOG.debug(f"Correct param: {correct_parameters}")
    LOG.debug(f"Query param: {query_parameters}")
    LOG.debug(f"Query param types: {[type(x) for x in query_parameters]}")

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

    LOG.info(f"Query FINAL param: {query_parameters}")
    LOG.info('Connecting to the DB to make the query.')
    variantsFound = await get_datasets(db_pool, query_parameters, include_dataset)
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
    accessible_datasets = ["EGAD00001000740", "EGAD00001000741"]  # NOTE we use the public_datasets because authentication is not implemented yet
    user_levels = ["PUBLIC"]  # NOTE we hardcode it because authentication is not implemented yet
    filtered_response = filter_response(beacon_response, ACCESS_LEVELS_DICT, accessible_datasets, user_levels, region2access)

    return filtered_response["beconGenomicRegionRequest"]