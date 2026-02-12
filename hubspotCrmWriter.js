/**
 * HubSpot CRM Writer Plugin
 *
 * Accepts incoming data as flat table rows or key-value lists,
 * transforms them into the HubSpot { "properties": { ... } } format,
 * and creates or updates the respective entity via the CRM v3 API.
 *
 * Supported entities: companies, contacts, deals, tickets
 */

// ---------------------------------------------------------------------------
// Auth helper (shared with extension.js reader)
// ---------------------------------------------------------------------------
function getAuthFromAdminConfig(authConfig) {
    if (!authConfig) {
        return { type: 'none', token: '', username: '', password: '' };
    }

    var properties = getConfigValue(authConfig, 'properties', null);
    var subType = getConfigValue(authConfig, 'subType', '');

    if (!properties) {
        return { type: 'none', token: '', username: '', password: '' };
    }

    if (subType === 'BEARER_TOKEN') {
        return {
            type: 'bearer',
            token: getConfigValue(properties, 'bearerToken', ''),
            username: '',
            password: ''
        };
    } else if (subType === 'BASIC_AUTH') {
        return {
            type: 'basic',
            token: '',
            username: getConfigValue(properties, 'basicAuthUsername', ''),
            password: getConfigValue(properties, 'basicAuthPassword', '')
        };
    }

    return { type: 'none', token: '', username: '', password: '' };
}

// ---------------------------------------------------------------------------
// Known HubSpot property names per entity (used for direct pass-through)
// ---------------------------------------------------------------------------
var HUBSPOT_PROPERTIES = {
    companies: [
        'name', 'domain', 'phone', 'city', 'state', 'country', 'zip',
        'address', 'address2', 'industry', 'numberofemployees', 'annualrevenue',
        'description', 'website', 'lifecyclestage', 'type', 'about_us',
        'founded_year', 'linkedinhandle', 'twitterhandle', 'facebookpage',
        'timezone', 'total_money_raised', 'hs_lead_status', 'hubspot_owner_id',
        'closedate', 'revenue_range', 'industry_group', 'state_code',
        'country_code'
    ],
    contacts: [
        'email', 'firstname', 'lastname', 'phone', 'mobilephone', 'fax',
        'jobtitle', 'company', 'city', 'state', 'country', 'zip', 'address',
        'website', 'industry', 'annualrevenue', 'lifecyclestage',
        'hs_lead_status', 'hubspot_owner_id', 'hs_email_domain', 'salutation',
        'date_of_birth', 'message', 'numemployees', 'hs_persona'
    ],
    deals: [
        'dealname', 'amount', 'dealstage', 'pipeline', 'closedate',
        'dealtype', 'description', 'hubspot_owner_id', 'hs_priority',
        'hs_forecast_amount', 'hs_forecast_probability',
        'hs_deal_stage_probability', 'hs_next_step'
    ],
    tickets: [
        'subject', 'content', 'hs_pipeline', 'hs_pipeline_stage',
        'hs_ticket_priority', 'hubspot_owner_id', 'hs_ticket_category',
        'hs_resolution', 'source_type'
    ]
};

// ---------------------------------------------------------------------------
// Alias mappings – common alternative field names → HubSpot property name
// ---------------------------------------------------------------------------
var PROPERTY_ALIASES = {
    companies: {
        'company_name': 'name', 'companyName': 'name', 'CompanyName': 'name',
        'AccountName': 'name', 'account_name': 'name', 'Name': 'name',
        'Domain': 'domain', 'website_domain': 'domain', 'companyDomain': 'domain',
        'WebSite': 'domain',
        'Phone': 'phone', 'telephone': 'phone', 'phoneNumber': 'phone',
        'PhoneNumber': 'phone', 'phone_number': 'phone',
        'City': 'city',
        'State': 'state', 'region': 'state', 'Region': 'state', 'StateCode': 'state',
        'Country': 'country', 'CountryCode': 'country',
        'Zip': 'zip', 'postalCode': 'zip', 'PostalCode': 'zip',
        'postal_code': 'zip', 'zipCode': 'zip', 'ZipCode': 'zip',
        'Address': 'address', 'street': 'address', 'Street': 'address',
        'streetAddress': 'address', 'StreetAddress': 'address', 'street_address': 'address',
        'Industry': 'industry',
        'employees': 'numberofemployees', 'Employees': 'numberofemployees',
        'employeeCount': 'numberofemployees', 'NumberOfEmployees': 'numberofemployees',
        'number_of_employees': 'numberofemployees',
        'revenue': 'annualrevenue', 'Revenue': 'annualrevenue',
        'annual_revenue': 'annualrevenue', 'AnnualRevenue': 'annualrevenue',
        'Description': 'description',
        'Website': 'website', 'URL': 'website', 'url': 'website', 'homepage': 'website',
        'lifecycle_stage': 'lifecyclestage', 'LifecycleStage': 'lifecyclestage',
        'companyType': 'type', 'company_type': 'type', 'Type': 'type',
        'owner_id': 'hubspot_owner_id', 'ownerId': 'hubspot_owner_id',
        'OwnerId': 'hubspot_owner_id'
    },
    contacts: {
        'Email': 'email', 'emailAddress': 'email', 'EmailAddress': 'email',
        'email_address': 'email', 'E_Mail': 'email',
        'first_name': 'firstname', 'FirstName': 'firstname', 'firstName': 'firstname',
        'givenName': 'firstname', 'GivenName': 'firstname',
        'last_name': 'lastname', 'LastName': 'lastname', 'lastName': 'lastname',
        'familyName': 'lastname', 'FamilyName': 'lastname', 'surname': 'lastname',
        'Surname': 'lastname',
        'Phone': 'phone', 'telephone': 'phone', 'phoneNumber': 'phone',
        'PhoneNumber': 'phone', 'phone_number': 'phone',
        'mobile': 'mobilephone', 'Mobile': 'mobilephone', 'cellphone': 'mobilephone',
        'CellPhone': 'mobilephone', 'cell_phone': 'mobilephone',
        'MobilePhone': 'mobilephone', 'mobile_phone': 'mobilephone',
        'Fax': 'fax', 'FaxNumber': 'fax', 'fax_number': 'fax',
        'job_title': 'jobtitle', 'JobTitle': 'jobtitle', 'jobTitle': 'jobtitle',
        'title': 'jobtitle', 'Title': 'jobtitle', 'position': 'jobtitle',
        'Position': 'jobtitle', 'FunctionName': 'jobtitle',
        'Company': 'company', 'companyName': 'company', 'CompanyName': 'company',
        'company_name': 'company', 'AccountName': 'company',
        'City': 'city',
        'State': 'state', 'region': 'state', 'Region': 'state',
        'Country': 'country', 'CountryCode': 'country',
        'Zip': 'zip', 'postalCode': 'zip', 'PostalCode': 'zip',
        'postal_code': 'zip', 'zipCode': 'zip', 'ZipCode': 'zip',
        'Address': 'address', 'street': 'address', 'Street': 'address',
        'streetAddress': 'address', 'StreetAddress': 'address',
        'Website': 'website', 'URL': 'website', 'url': 'website',
        'lifecycle_stage': 'lifecyclestage', 'LifecycleStage': 'lifecyclestage',
        'owner_id': 'hubspot_owner_id', 'ownerId': 'hubspot_owner_id',
        'OwnerId': 'hubspot_owner_id',
        'Salutation': 'salutation', 'TitleOfCourtesy': 'salutation'
    },
    deals: {
        'deal_name': 'dealname', 'DealName': 'dealname', 'name': 'dealname',
        'Name': 'dealname', 'subject': 'dealname', 'Subject': 'dealname',
        'OpportunityName': 'dealname',
        'Amount': 'amount', 'value': 'amount', 'Value': 'amount',
        'dealAmount': 'amount', 'deal_amount': 'amount',
        'ExpectedRevenueAmount': 'amount',
        'deal_stage': 'dealstage', 'DealStage': 'dealstage', 'stage': 'dealstage',
        'Stage': 'dealstage', 'SalesPhaseCode': 'dealstage',
        'Pipeline': 'pipeline',
        'close_date': 'closedate', 'CloseDate': 'closedate',
        'closingDate': 'closedate', 'ClosingDate': 'closedate',
        'ExpectedCloseDate': 'closedate', 'expected_close_date': 'closedate',
        'deal_type': 'dealtype', 'DealType': 'dealtype', 'type': 'dealtype',
        'Type': 'dealtype',
        'Description': 'description',
        'owner_id': 'hubspot_owner_id', 'ownerId': 'hubspot_owner_id',
        'OwnerId': 'hubspot_owner_id',
        'priority': 'hs_priority', 'Priority': 'hs_priority'
    },
    tickets: {
        'Subject': 'subject', 'name': 'subject', 'Name': 'subject',
        'title': 'subject', 'Title': 'subject', 'ticket_name': 'subject',
        'Content': 'content', 'description': 'content', 'Description': 'content',
        'body': 'content', 'Body': 'content',
        'pipeline': 'hs_pipeline', 'Pipeline': 'hs_pipeline',
        'stage': 'hs_pipeline_stage', 'Stage': 'hs_pipeline_stage',
        'status': 'hs_pipeline_stage', 'Status': 'hs_pipeline_stage',
        'pipeline_stage': 'hs_pipeline_stage',
        'priority': 'hs_ticket_priority', 'Priority': 'hs_ticket_priority',
        'ticket_priority': 'hs_ticket_priority',
        'category': 'hs_ticket_category', 'Category': 'hs_ticket_category',
        'owner_id': 'hubspot_owner_id', 'ownerId': 'hubspot_owner_id',
        'OwnerId': 'hubspot_owner_id'
    }
};

// Default unique property used to search for existing records (upsert lookup)
var DEFAULT_UNIQUE_PROPERTIES = {
    companies: 'domain',
    contacts: 'email',
    deals: 'dealname',
    tickets: 'subject'
};

// Keys that should never be forwarded as HubSpot properties
var SKIP_KEYS = {
    '__metadata': true, 'ObjectID': true, 'ETag': true,
    'uri': true, 'type': false // 'type' can be valid for companies
};

// ---------------------------------------------------------------------------
// Writer implementation
// ---------------------------------------------------------------------------
function hubspotCrmWriter(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubspot.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var lookupProperty = getConfigValue(config, 'lookupProperty', '');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var headers = {};
    var recordCount = 0;

    // Resolve entity-specific helpers once
    var entityAliases = PROPERTY_ALIASES[entity] || {};
    var entityProperties = HUBSPOT_PROPERTIES[entity] || [];
    var uniqueProperty = lookupProperty || DEFAULT_UNIQUE_PROPERTIES[entity] || '';

    // Build a quick set for O(1) lookup of known HubSpot property names
    var knownPropertySet = {};
    for (var i = 0; i < entityProperties.length; i++) {
        knownPropertySet[entityProperties[i]] = true;
    }

    // -- Data transformation helpers ----------------------------------------

    /**
     * Converts a key-value list (array of {key,value} pairs) to a flat object.
     * Recognises common key/value field names.
     */
    function keyValueListToObject(list) {
        var obj = {};
        for (var idx = 0; idx < list.length; idx++) {
            var item = list[idx];
            if (!item || typeof item !== 'object') continue;
            var k = item.key || item.Key || item.name || item.Name
                 || item.property || item.Property || item.field || item.Field || '';
            var v = item.value !== undefined ? item.value
                  : (item.Value !== undefined ? item.Value : '');
            if (k) {
                obj[String(k)] = v;
            }
        }
        return obj;
    }

    /**
     * Normalises any incoming record format into a flat key→value object.
     *   - JSON string → parsed
     *   - Array of {key,value} → flat object
     *   - { properties: { … } } → inner object
     *   - Flat object → as-is
     */
    function normalizeToFlat(record) {
        if (!record) return {};
        if (typeof record === 'string') {
            record = JSON.parse(record);
        }
        if (Array.isArray(record)) {
            return keyValueListToObject(record);
        }
        if (record.properties && typeof record.properties === 'object'
            && !Array.isArray(record.properties)) {
            return record.properties;
        }
        return record;
    }

    /**
     * Maps a single incoming key to the correct HubSpot property name.
     */
    function mapPropertyName(key) {
        if (knownPropertySet[key]) return key;
        if (entityAliases[key]) return entityAliases[key];
        var lower = key.toLowerCase();
        if (knownPropertySet[lower]) return lower;
        // Pass through unknown keys (custom properties)
        return lower;
    }

    /**
     * Transforms any incoming record into a flat HubSpot properties object
     * ready to be wrapped as { "properties": { … } }.
     */
    function transformToProperties(record) {
        var flat = normalizeToFlat(record);
        var properties = {};

        for (var key in flat) {
            if (!flat.hasOwnProperty(key)) continue;

            var value = flat[key];

            // Skip empty values
            if (value === null || value === undefined || value === '') continue;

            // Skip internal _chioro attributes
            if (key.indexOf('_chioro') === 0) continue;

            // Skip OData metadata keys
            if (key === '__metadata' || key === 'ObjectID' || key === 'ETag'
                || key === 'uri') continue;

            var hubspotKey = mapPropertyName(key);
            properties[hubspotKey] = String(value);
        }

        return properties;
    }

    // -- HubSpot API helpers ------------------------------------------------

    function buildObjectUrl(recordId) {
        var url = baseUrl + '/crm/v3/objects/' + entity;
        if (recordId) {
            url += '/' + encodeURIComponent(String(recordId));
        }
        return url;
    }

    function buildSearchUrl() {
        return baseUrl + '/crm/v3/objects/' + entity + '/search';
    }

    function findExistingId(properties) {
        if (!uniqueProperty) return '';
        var uniqueValue = properties[uniqueProperty];
        if (!uniqueValue) return '';

        var payload = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: uniqueProperty,
                            operator: "EQ",
                            value: String(uniqueValue)
                        }
                    ]
                }
            ],
            properties: [uniqueProperty],
            limit: 1
        };

        var data = postJson(buildSearchUrl(), payload, headers);
        if (data && data.results && data.results.length > 0 && data.results[0].id) {
            return String(data.results[0].id);
        }
        return '';
    }

    function createRecord(properties) {
        var payload = { properties: properties };
        postJson(buildObjectUrl(''), payload, headers);
    }

    function updateRecord(recordId, properties) {
        var payload = { properties: properties };
        _apiFetcher.patchUrl(
            buildObjectUrl(recordId),
            JSON.stringify(payload),
            headers
        );
    }

    // -- Writer interface ---------------------------------------------------

    return {
        open: function () {
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            };

            var auth = getAuthFromAdminConfig(authConfig);
            if (auth.type === 'bearer' && auth.token) {
                headers["Authorization"] = "Bearer " + auth.token;
            } else if (auth.type === 'basic' && auth.username && auth.password) {
                headers["Authorization"] = "Basic " + base64Encode(
                    auth.username + ':' + auth.password
                );
            }
        },

        writeRecord: function (record) {
            var properties = transformToProperties(record);

            // Nothing useful to write
            var hasProps = false;
            for (var k in properties) {
                if (properties.hasOwnProperty(k)) { hasProps = true; break; }
            }
            if (!hasProps) return;

            var existingId = findExistingId(properties);

            if (existingId) {
                updateRecord(existingId, properties);
            } else {
                createRecord(properties);
            }

            recordCount++;
            if (journal && journal.onProgress) {
                journal.onProgress(recordCount);
            }
        },

        close: function () {
            recordCount = 0;
        }
    };
}

module.exports = hubspotCrmWriter;
