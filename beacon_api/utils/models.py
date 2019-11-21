
from .. import __id__, __beacon_name__, __apiVersion__, __org_id__, __org_name__, __org_description__, __org_adress__, __org_welcomeUrl__, __org_contactUrl__, __org_logoUrl__, __org_info__
from .. import __description__, __version__, __welcomeUrl__, __alternativeUrl__, __createDateTime__, __updateDateTime__
from .. import __service__, __serviceUrl__, __entryPoint__, __open__, __service_type__, __documentationUrl__, __environment__


organization = {
    'id': __id__,
    'name': __beacon_name__,
    'description': __org_description__,
    'address': __org_adress__,
    'welcomeUrl': __org_welcomeUrl__,
    'contactUrl': __org_contactUrl__,
    'logoUrl': __org_logoUrl__,
    'info': __org_info__,
}

def Beacon_v1(host):
    Beacon_v1 = {
        'id': '.'.join(reversed(host.split('.'))),
        'name': __beacon_name__,
        'serviceType': __service__,
        'apiVersion': __apiVersion__,
        'serviceUrl': __serviceUrl__,
        'entryPoint': __entryPoint__,
        'organization': organization,
        'description': __description__,
        'version': __version__,
        'open': __open__,
        'welcomeUrl': __welcomeUrl__,
        'alternativeUrl': __alternativeUrl__,
        'createDateTime': __createDateTime__,
        'updateDateTime': __updateDateTime__,
    }
    return Beacon_v1

def GA4GH_ServiceInfo_v01(host):
    GA4GH_ServiceInfo_v01 = {
        'id': '.'.join(reversed(host.split('.'))),
        'name': __beacon_name__,
        'type': __service_type__,
        'description': __description__,
        "organization": {'name': __org_name__,
                        'url': __org_welcomeUrl__},
        'contactUrl': __org_contactUrl__,
        'documentationUrl': __documentationUrl__,
        'createDateTime': __createDateTime__,
        'updateDateTime': __updateDateTime__,
        'environment': __environment__,
        'version': __version__,
    }
    return GA4GH_ServiceInfo_v01
