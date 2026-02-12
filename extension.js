const Toolpackage = require('chioro-toolbox/toolpackage')
const hubspotCrmWriter = require('./hubspotCrmWriter')

const tools = new Toolpackage("Pipe Reader Tools")
tools.description = 'Plugin for reading pipe-separated files'

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

/**
 * SAP C4C OData reader.
 *
 * Supports $filter, $expand, and pagination via $top/$skip.
 * The "top" arg limits total records returned; "pageSize" controls page size.
 */
function sapC4cCorporateAccountsReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://my360473.crm.ondemand.com');
    var endpoint = getConfigValue(
        config,
        'endpoint',
        '/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection'
    );
    var topLimit = parseInt(getConfigValue(config, 'top', 0), 10);
    if (!topLimit || topLimit < 1) {
        topLimit = 0;
    }
    var pageSize = parseInt(getConfigValue(config, 'pageSize', 100), 10);
    if (!pageSize || pageSize < 1) {
        pageSize = 100;
    }

    var filter = getConfigValue(config, 'filter', '');
    var expands = getConfigValue(config, 'extends', '');
    if (!expands) {
        expands = getConfigValue(config, 'expand', '');
    }

    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);

    var skip = 0;
    var hasMore = true;
    var buffer = [];
    var bufferIndex = 0;
    var recordCount = 0;
    var headers = null;

    function buildHeaders() {
        var hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        };

        if (auth.type === 'basic' && auth.username && auth.password) {
            hdrs["Authorization"] = "Basic " + base64Encode(auth.username + ":" + auth.password);
        }

        return hdrs;
    }

    function normalizeRecords(data) {
        if (!data) return [];
        if (data.d && Array.isArray(data.d.results)) return data.d.results;
        if (Array.isArray(data.value)) return data.value;
        if (Array.isArray(data.results)) return data.results;
        return [];
    }

    function buildUrl() {
        var effectiveTop = pageSize;
        if (topLimit > 0) {
            var remaining = topLimit - recordCount;
            if (remaining <= 0) {
                return null;
            }
            if (remaining < effectiveTop) {
                effectiveTop = remaining;
            }
        }

        var query = [];
        query.push("$top=" + encodeURIComponent(String(effectiveTop)));
        query.push("$skip=" + encodeURIComponent(String(skip)));
        query.push("sap-label=true");

        if (filter) {
            query.push("$filter=" + encodeURIComponent(String(filter)));
        }
        if (expands) {
            query.push("$expand=" + encodeURIComponent(String(expands)));
        }

        return baseUrl + endpoint + "?" + query.join("&");
    }

    function fetchNextPage() {
        if (!hasMore) return;

        var url = buildUrl();
        if (!url) {
            hasMore = false;
            return;
        }
        var data = getJson(url, headers);

        buffer = normalizeRecords(data);
        bufferIndex = 0;

        if (buffer.length < pageSize) {
            hasMore = false;
        } else {
            skip += pageSize;
        }
    }

    return {
        open: function() {
            skip = 0;
            hasMore = true;
            buffer = [];
            bufferIndex = 0;
            recordCount = 0;
            headers = buildHeaders();
        },

        readRecords: function*() {
            while (true) {
                if (bufferIndex >= buffer.length) {
                    if (!hasMore) {
                        break;
                    }
                    fetchNextPage();
                    if (buffer.length === 0 && !hasMore) {
                        break;
                    }
                }

                while (bufferIndex < buffer.length) {
                    if (topLimit > 0 && recordCount >= topLimit) {
                        hasMore = false;
                        break;
                    }
                    recordCount++;
                    if (journal && journal.onProgress) {
                        journal.onProgress(recordCount);
                    }
                    yield buffer[bufferIndex++];
                }
            }
        },

        close: function() {
            skip = 0;
            hasMore = true;
            buffer = [];
            bufferIndex = 0;
            recordCount = 0;
            headers = null;
        }
    };
}

tools.add({
    id: "sapC4cCorporateAccountsReader",
    impl: sapC4cCorporateAccountsReader,
    aliases: {
        en: "sapC4cCorporateAccountsReader",
        de: "sapC4cCorporateAccountsReader"
    },
    simpleDescription: {
        en: "Reads SAP C4C OData entities",
        de: "Liest SAP C4C OData Entitäten"
    },
    args: [
        {
            key: "baseUrl",
            label_en: "API Base URL",
            label_de: "API Basis-URL",
            type: "text",
            required: true,
            default: "https://my360473.crm.ondemand.com",
            desc_en: "Base URL of the SAP C4C tenant"
        },
        {
            key: "endpoint",
            label_en: "Endpoint",
            label_de: "Endpunkt",
            type: "select",
            options: [
                "/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection",
                "/sap/c4c/odata/v1/c4codataapi/ContactCollection",
                "/sap/c4c/odata/v1/c4codataapi/LeadCollection",
                "/sap/c4c/odata/v1/c4codataapi/OpportunityCollection"
            ],
            default: "/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection",
            desc_en: "OData endpoint path"
        },
        {
            key: "top",
            label_en: "Top",
            label_de: "Top",
            type: "text",
            default: "0",
            desc_en: "Maximum number of records to return ($top). 0 = no limit"
        },
        {
            key: "pageSize",
            label_en: "Page Size",
            label_de: "Seitengröße",
            type: "text",
            default: "100",
            desc_en: "Number of records per page when paging"
        },
        {
            key: "filter",
            label_en: "Filter ($filter)",
            label_de: "Filter ($filter)",
            type: "text",
            desc_en: "Optional OData $filter expression to fetch specific accounts"
        },
        {
            key: "extends",
            label_en: "Expand ($expand)",
            label_de: "Erweiterungen ($expand)",
            type: "text",
            desc_en: "Comma-separated related entities, e.g. CorporateAccountAddresses"
        },
        {
            key: "authConfig",
            label_en: "Authentication",
            label_de: "Authentifizierung",
            type: "adminconfig",
            subType: "BASIC_AUTH",
            required: true,
            desc_en: "Select Basic Auth credentials from AdminConfig"
        }
    ],
    tags: ["reader", "dynamic-plugin"],
    hideInToolbox: true,
    tests: () => {}
})


tools.add({
    id: "hubspotCrmWriter",
    impl: hubspotCrmWriter,
    aliases: {
        en: "hubspotCrmWriter",
        de: "hubspotCrmWriter"
    },
    simpleDescription: {
        en: "HubSpot CRM Writer (Companies, Contacts, Deals, Tickets)",
        de: "HubSpot CRM Writer (Unternehmen, Kontakte, Deals, Tickets)"
    },
    args: [
        {
            key: "baseUrl",
            label_en: "API Base URL",
            label_de: "API Basis-URL",
            type: "text",
            required: true,
            default: "https://api.hubspot.com",
            desc_en: "Base URL of the HubSpot API"
        },
        {
            key: "entity",
            label_en: "Entity",
            label_de: "Entität",
            type: "select",
            options: ["companies", "contacts", "deals", "tickets"],
            default: "companies",
            desc_en: "Which HubSpot CRM entity to write"
        },
        {
            key: "lookupProperty",
            label_en: "Lookup Property (optional)",
            label_de: "Suchfeld (optional)",
            type: "text",
            default: "",
            desc_en: "Property used to find existing records for upsert. Defaults: domain (companies), email (contacts), dealname (deals), subject (tickets)"
        },
        {
            key: "authConfig",
            label_en: "Authentication",
            label_de: "Authentifizierung",
            type: "adminconfig",
            subType: "BEARER_TOKEN",
            required: true,
            desc_en: "Select Bearer Token from AdminConfig",
            desc_de: "Bearer Token aus AdminConfig auswählen"
        }
    ],
    tags: ["dynamic-plugin", "writer"],
    hideInToolbox: true,
    tests: () => {}
})


// Export all tools using the standard pattern
tools.exportAll(exports)
