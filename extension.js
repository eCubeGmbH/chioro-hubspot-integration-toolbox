const Toolpackage = require('chioro-toolbox/toolpackage')

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
 */
function sapC4cCorporateAccountsReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://my360473.crm.ondemand.com');
    var endpoint = getConfigValue(
        config,
        'endpoint',
        '/sap/c4c/odata/v1/c4codataapi/CorporateAccountCollection'
    );
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

    var initialSkip = parseInt(getConfigValue(config, 'skip', 0), 10);
    if (!initialSkip || initialSkip < 0) {
        initialSkip = 0;
    }

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
        var query = [];
        query.push("$top=" + encodeURIComponent(String(pageSize)));
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
            skip = initialSkip;
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
            key: "pageSize",
            label_en: "Page Size ($top)",
            label_de: "Seitengröße ($top)",
            type: "text",
            default: "100",
            desc_en: "Number of records per page"
        },
        {
            key: "skip",
            label_en: "Skip ($skip)",
            label_de: "Überspringen ($skip)",
            type: "text",
            default: "0",
            desc_en: "Number of records to skip before reading"
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
    tags: ["reader", "api", "sap", "c4c", "accounts", "contacts", "leads", "opportunities"],
    hideInToolbox: true,
    tests: () => {}
})


/**
 * HubSpot CRM writer for Companies, Contacts, and Deals.
 *
 * Behavior:
 * - If a record ID (or custom idProperty) is provided, the writer first tries to retrieve the record.
 * - If found -> update via PATCH.
 * - If not found -> create via POST.
 */
function hubspotCrmWriter(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubspot.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var idField = getConfigValue(config, 'idField', 'id');
    var idProperty = getConfigValue(config, 'idProperty', '');

    var authConfig = getConfigValue(config, 'authConfig', null);
    var headers = {};
    var recordCount = 0;

    function getEntityPath() {
        if (entity === 'contacts') return 'contacts';
        if (entity === 'deals') return 'deals';
        return 'companies';
    }

    function buildUrl(recordId) {
        var url = baseUrl + '/crm/v3/objects/' + getEntityPath();
        if (recordId) {
            url += '/' + encodeURIComponent(String(recordId));
        }
        if (idProperty) {
            url += '?idProperty=' + encodeURIComponent(String(idProperty));
        }
        return url;
    }

    function getRecordId(record) {
        if (!record) return '';
        if (record.id !== undefined && record.id !== null && String(record.id) !== '') {
            return String(record.id);
        }
        if (record.recordId !== undefined && record.recordId !== null && String(record.recordId) !== '') {
            return String(record.recordId);
        }
        if (idField && record[idField] !== undefined && record[idField] !== null && String(record[idField]) !== '') {
            return String(record[idField]);
        }
        if (record.properties && idField && record.properties[idField] !== undefined && record.properties[idField] !== null) {
            return String(record.properties[idField]);
        }
        return '';
    }

    function buildProperties(record) {
        var props = {};
        if (record && record.properties && typeof record.properties === 'object') {
            for (var key in record.properties) {
                if (!record.properties.hasOwnProperty(key)) continue;
                if (key === idField || key === 'id' || key === 'recordId') continue;
                props[key] = record.properties[key];
            }
            return props;
        }

        if (record && typeof record === 'object') {
            for (var k in record) {
                if (!record.hasOwnProperty(k)) continue;
                if (k === idField || k === 'id' || k === 'recordId' || k === 'properties') continue;
                props[k] = record[k];
            }
        }
        return props;
    }

    function isNotFound(err) {
        if (!err) return false;
        var msg = String(err.message || err);
        return msg.indexOf('404') !== -1 || msg.indexOf('Not Found') !== -1;
    }

    function retrieveExists(recordId) {
        if (!recordId) return false;
        var url = buildUrl(recordId);
        try {
            getJson(url, headers);
            return true;
        } catch (err) {
            if (isNotFound(err)) {
                return false;
            }
            throw err;
        }
    }

    function createRecord(properties) {
        var url = buildUrl('');
        var payload = { properties: properties };
        postJson(url, payload, headers);
    }

    function updateRecord(recordId, properties) {
        var url = buildUrl(recordId);
        var payload = { properties: properties };
        _apiFetcher.patchUrl(url, JSON.stringify(payload), headers);
    }

    return {
        open: function() {
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            };

            var auth = getAuthFromAdminConfig(authConfig);
            if (auth.type === 'bearer' && auth.token) {
                headers["Authorization"] = "Bearer " + auth.token;
            } else if (auth.type === 'basic' && auth.username && auth.password) {
                headers["Authorization"] = "Basic " + base64Encode(auth.username + ':' + auth.password);
            }
        },

        writeRecord: function(recordJson) {
            var record = recordJson;
            if (typeof recordJson === 'string') {
                record = JSON.parse(recordJson);
            }

            var recordId = getRecordId(record);
            var properties = buildProperties(record);

            if (recordId && retrieveExists(recordId)) {
                updateRecord(recordId, properties);
            } else {
                createRecord(properties);
            }

            recordCount++;
            if (journal && journal.onProgress) {
                journal.onProgress(recordCount);
            }
        },

        close: function() {
            recordCount = 0;
        }
    };
}

tools.add({
    id: "hubspotCrmWriter",
    impl: hubspotCrmWriter,
    aliases: {
        en: "hubspotCrmWriter",
        de: "hubspotCrmWriter"
    },
    simpleDescription: {
        en: "HubSpot CRM Writer (Companies, Contacts, Deals)",
        de: "HubSpot CRM Writer (Unternehmen, Kontakte, Deals)"
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
            options: ["companies", "contacts", "deals"],
            default: "companies",
            desc_en: "Which HubSpot CRM entity to write"
        },
        {
            key: "idField",
            label_en: "Record ID Field",
            label_de: "Record-ID Feld",
            type: "text",
            default: "id",
            desc_en: "Field in the incoming record that contains the unique ID"
        },
        {
            key: "idProperty",
            label_en: "ID Property (optional)",
            label_de: "ID Property (optional)",
            type: "text",
            default: "",
            desc_en: "Use a custom unique identifier property (leave empty to use record ID)"
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
    tags: ["writer", "api", "hubspot", "crm", "companies", "contacts", "deals"],
    hideInToolbox: true,
    tests: () => {}
})


// Export all tools using the standard pattern
tools.exportAll(exports)
