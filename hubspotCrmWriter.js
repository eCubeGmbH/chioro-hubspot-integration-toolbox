/**
 * HubSpot CRM Writer Plugin
 *
 * Accepts incoming data as flat table rows or key-value lists,
 * transforms them into the HubSpot { "properties": { ... } } format,
 * and creates or updates the respective entity via the CRM v3 API.
 *
 * Supported entities: companies, contacts, deals, tickets
 *
 * Contact write logic (enhanced):
 *  1. Look up existing contact by email; if not found, by external_contact_id.
 *  2. If found  → PATCH only properties that are not yet set in HubSpot.
 *  3. If not found → POST (create).
 *  4. In both cases, if AccountID is present on the incoming record, look up
 *     the matching HubSpot company by external_account_id and associate the
 *     contact with that company.
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
        'country_code', 'external_account_id'
    ],
    contacts: [
        'email', 'firstname', 'lastname', 'phone', 'mobilephone', 'fax',
        'jobtitle', 'company', 'city', 'state', 'country', 'zip', 'address',
        'website', 'industry', 'annualrevenue', 'lifecyclestage',
        'hs_lead_status', 'hubspot_owner_id', 'hs_email_domain', 'salutation',
        'date_of_birth', 'message', 'numemployees', 'hs_persona',
        'external_contact_id'
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
        'Salutation': 'salutation', 'TitleOfCourtesy': 'salutation',
        'ExternalContactId': 'external_contact_id',
        'externalContactId': 'external_contact_id',
        'external_contact_id': 'external_contact_id'
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

// Keys that should never be forwarded as HubSpot contact properties.
// AccountID is a relational field used only to look up the associated company.
var CONTACT_SKIP_KEYS = {
    'AccountID': true,
    'accountid': true,
    'account_id': true,
    'AccountId': true
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

            // Skip contact-specific relational keys that must not be sent as
            // HubSpot properties (AccountID is used only for company association)
            if (entity === 'contacts' && CONTACT_SKIP_KEYS[key]) continue;

            var hubspotKey = mapPropertyName(key);

            // After lowercasing, also skip if the lowercased variant is a
            // contact skip key (e.g. 'accountid')
            if (entity === 'contacts' && CONTACT_SKIP_KEYS[hubspotKey]) continue;

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

    /**
     * Searches HubSpot for an existing company by its external_account_id
     * custom property. Returns the HubSpot record id if found, '' otherwise.
     *
     * @param {object} flatRecord  Normalised flat key→value record
     * @returns {string} HubSpot company id or ''
     */
    function findCompanyByExternalAccountId(flatRecord) {
        var externalAccountId = flatRecord['external_account_id']
            || flatRecord['ExternalAccountId']
            || flatRecord['externalAccountId']
            || flatRecord['externalaccountid']
            || '';

        if (!externalAccountId) return '';

        var payload = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: 'external_account_id',
                            operator: 'EQ',
                            value: String(externalAccountId)
                        }
                    ]
                }
            ],
            properties: ['external_account_id'],
            limit: 1
        };

        var data = postJson(
            baseUrl + '/crm/v3/objects/companies/search',
            payload,
            headers
        );
        if (data && data.results && data.results.length > 0 && data.results[0].id) {
            return String(data.results[0].id);
        }
        return '';
    }

    /**
     * Fetches the current properties of an existing company from HubSpot.
     * Only requests the property names we are interested in to keep the
     * response payload small.
     *
     * @param {string} companyId   HubSpot company record id
     * @param {string[]} propertyNames  Property names to retrieve
     * @returns {object} Map of propertyName → current value (may be empty string)
     */
    function fetchExistingCompanyProperties(companyId, propertyNames) {
        if (!companyId || !propertyNames || propertyNames.length === 0) return {};
        var url = buildObjectUrl(companyId) + '?properties=' + propertyNames.join(',');
        try {
            var data = getJson(url, headers);
            if (data && data.properties) {
                return data.properties;
            }
        } catch (e) {
            // If the fetch fails we fall back to updating all properties
        }
        return {};
    }

    /**
     * Returns only the properties from desiredProperties whose value is not
     * already set in existingProperties.  A property is considered "already
     * set" when its current HubSpot value is a non-empty, non-null string.
     *
     * @param {object} desiredProperties  Properties we want to write
     * @param {object} existingProperties Properties currently stored in HubSpot
     * @returns {object} Subset of desiredProperties that should be written
     */
    function filterUnsetProperties(desiredProperties, existingProperties) {
        var filtered = {};
        for (var key in desiredProperties) {
            if (!desiredProperties.hasOwnProperty(key)) continue;
            var existingValue = existingProperties[key];
            // Only include the property when the existing value is absent or empty
            if (existingValue === null || existingValue === undefined || existingValue === '') {
                filtered[key] = desiredProperties[key];
            }
        }
        return filtered;
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

    /**
     * Updates a company record, but only writes properties that are not
     * already set in HubSpot.  Skips the PATCH entirely when nothing new
     * needs to be written.
     *
     * @param {string} companyId   HubSpot company record id
     * @param {object} properties  Properties we want to write
     */
    function updateCompanyWithUnsetOnly(companyId, properties) {
        var propertyNames = Object.keys(properties);
        var existingProps = fetchExistingCompanyProperties(companyId, propertyNames);
        var propsToUpdate = filterUnsetProperties(properties, existingProps);

        // Check whether there is anything left to update
        var hasProps = false;
        for (var pk in propsToUpdate) {
            if (propsToUpdate.hasOwnProperty(pk)) { hasProps = true; break; }
        }
        if (hasProps) {
            updateRecord(companyId, propsToUpdate);
        }
    }

    // -- Contact-specific helpers -------------------------------------------

    /**
     * Searches HubSpot contacts by email address.
     *
     * @param {string} email  Email address to search for
     * @returns {string} HubSpot contact id or ''
     */
    function findContactByEmail(email) {
        if (!email) return '';
        var payload = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: 'email',
                            operator: 'EQ',
                            value: String(email)
                        }
                    ]
                }
            ],
            properties: ['email'],
            limit: 1
        };
        var data = postJson(
            baseUrl + '/crm/v3/objects/contacts/search',
            payload,
            headers
        );
        if (data && data.results && data.results.length > 0 && data.results[0].id) {
            return String(data.results[0].id);
        }
        return '';
    }

    /**
     * Searches HubSpot contacts by the custom external_contact_id property.
     *
     * @param {string} externalId  External contact id to search for
     * @returns {string} HubSpot contact id or ''
     */
    function findContactByExternalContactId(externalId) {
        if (!externalId) return '';
        var payload = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: 'external_contact_id',
                            operator: 'EQ',
                            value: String(externalId)
                        }
                    ]
                }
            ],
            properties: ['external_contact_id'],
            limit: 1
        };
        var data = postJson(
            baseUrl + '/crm/v3/objects/contacts/search',
            payload,
            headers
        );
        if (data && data.results && data.results.length > 0 && data.results[0].id) {
            return String(data.results[0].id);
        }
        return '';
    }

    /**
     * Fetches the current properties of an existing contact from HubSpot.
     *
     * @param {string} contactId     HubSpot contact record id
     * @param {string[]} propertyNames  Property names to retrieve
     * @returns {object} Map of propertyName → current value (may be empty string)
     */
    function fetchExistingContactProperties(contactId, propertyNames) {
        if (!contactId || !propertyNames || propertyNames.length === 0) return {};
        var url = baseUrl + '/crm/v3/objects/contacts/'
            + encodeURIComponent(String(contactId))
            + '?properties=' + propertyNames.join(',');
        try {
            var data = getJson(url, headers);
            if (data && data.properties) {
                return data.properties;
            }
        } catch (e) {
            // Fall back to updating all properties if fetch fails
        }
        return {};
    }

    /**
     * PATCHes a contact, but only sends properties that are not yet set.
     * Skips the PATCH entirely when nothing new needs to be written.
     *
     * @param {string} contactId   HubSpot contact record id
     * @param {object} properties  Properties we want to write
     */
    function updateContactWithUnsetOnly(contactId, properties) {
        var propertyNames = Object.keys(properties);
        var existingProps = fetchExistingContactProperties(contactId, propertyNames);
        var propsToUpdate = filterUnsetProperties(properties, existingProps);

        var hasProps = false;
        for (var pk in propsToUpdate) {
            if (propsToUpdate.hasOwnProperty(pk)) { hasProps = true; break; }
        }
        if (hasProps) {
            updateRecord(contactId, propsToUpdate);
        }
    }

    /**
     * Looks up a HubSpot company whose external_account_id equals the given
     * accountId.  Returns the HubSpot company id or '' if not found.
     *
     * Used to resolve the AccountID field on incoming contact records.
     *
     * @param {string} accountId  Value of the contact's AccountID field
     * @returns {string} HubSpot company id or ''
     */
    function findCompanyByAccountId(accountId) {
        if (!accountId) return '';
        var payload = {
            filterGroups: [
                {
                    filters: [
                        {
                            propertyName: 'external_account_id',
                            operator: 'EQ',
                            value: String(accountId)
                        }
                    ]
                }
            ],
            properties: ['external_account_id'],
            limit: 1
        };
        var data = postJson(
            baseUrl + '/crm/v3/objects/companies/search',
            payload,
            headers
        );
        if (data && data.results && data.results.length > 0 && data.results[0].id) {
            return String(data.results[0].id);
        }
        return '';
    }

    /**
     * Creates a contact with an inline association to the given company.
     * The association is embedded in the POST body, which is supported by
     * the HubSpot CRM v3 objects create endpoint.
     *
     * Association type 1 = HUBSPOT_DEFINED contact-to-company (primary company).
     *
     * @param {object} properties  Contact properties to create
     * @param {string} companyId   HubSpot company id to associate with
     */
    function createContactWithAssociation(properties, companyId) {
        var payload = {
            properties: properties,
            associations: [
                {
                    to: { id: companyId },
                    types: [
                        {
                            associationCategory: 'HUBSPOT_DEFINED',
                            associationTypeId: 1
                        }
                    ]
                }
            ]
        };
        postJson(baseUrl + '/crm/v3/objects/contacts', payload, headers);
    }

    /**
     * Associates an existing contact with a company using the v3 batch
     * associations endpoint.  The PATCH endpoint does not support inline
     * associations, so a separate call is required for existing contacts.
     *
     * @param {string} contactId  HubSpot contact id
     * @param {string} companyId  HubSpot company id
     */
    function associateContactWithCompany(contactId, companyId) {
        var payload = {
            inputs: [
                {
                    from: { id: contactId },
                    to: { id: companyId },
                    type: 'contact_to_company'
                }
            ]
        };
        postJson(
            baseUrl + '/crm/v3/associations/contacts/companies/batch/create',
            payload,
            headers
        );
    }

    /**
     * Extracts the AccountID value from a flat (normalised) contact record.
     * Checks multiple common field name variants.
     *
     * @param {object} flatRecord  Normalised flat key→value record
     * @returns {string} AccountID value or ''
     */
    function extractAccountId(flatRecord) {
        return flatRecord['AccountID']
            || flatRecord['AccountId']
            || flatRecord['account_id']
            || flatRecord['accountid']
            || '';
    }

    /**
     * Extracts the external_contact_id value from a flat contact record.
     *
     * @param {object} flatRecord  Normalised flat key→value record
     * @returns {string} external_contact_id value or ''
     */
    function extractExternalContactId(flatRecord) {
        return flatRecord['external_contact_id']
            || flatRecord['ExternalContactId']
            || flatRecord['externalContactId']
            || '';
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

            if (entity === 'companies') {
                // Look up by external_account_id; only update unset properties
                var flatRecord = normalizeToFlat(record);
                var existingId = findCompanyByExternalAccountId(flatRecord);

                if (existingId) {
                    updateCompanyWithUnsetOnly(existingId, properties);
                } else {
                    createRecord(properties);
                }

            } else if (entity === 'contacts') {
                // Enhanced contact logic:
                //  1. Find existing contact by email, then by external_contact_id
                //  2. Update (unset-only) or create
                //  3. Associate with company if AccountID is present

                var flatContact = normalizeToFlat(record);

                // Step 1: locate existing contact
                var contactId = '';
                var email = properties['email'] || '';
                if (email) {
                    contactId = findContactByEmail(email);
                }
                if (!contactId) {
                    var extContactId = extractExternalContactId(flatContact);
                    if (extContactId) {
                        contactId = findContactByExternalContactId(extContactId);
                    }
                }

                // Step 2: resolve company association (if AccountID is set)
                var accountId = extractAccountId(flatContact);
                var companyHubspotId = '';
                if (accountId) {
                    companyHubspotId = findCompanyByAccountId(accountId);
                }

                // Step 3: create or update
                if (contactId) {
                    // Update existing contact with only unset properties
                    updateContactWithUnsetOnly(contactId, properties);
                    // Associate with company separately (PATCH does not support
                    // inline associations)
                    if (companyHubspotId) {
                        associateContactWithCompany(contactId, companyHubspotId);
                    }
                } else {
                    // Create new contact
                    if (companyHubspotId) {
                        createContactWithAssociation(properties, companyHubspotId);
                    } else {
                        createRecord(properties);
                    }
                }

            } else {
                // Default upsert logic for deals, tickets, etc.
                var existingId = findExistingId(properties);
                if (existingId) {
                    updateRecord(existingId, properties);
                } else {
                    createRecord(properties);
                }
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
