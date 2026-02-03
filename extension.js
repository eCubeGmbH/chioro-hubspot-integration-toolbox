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
 * SAP C4C Corporate Accounts reader (OData).
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
        en: "Reads Corporate Accounts from SAP C4C OData",
        de: "Liest Corporate Accounts aus SAP C4C OData"
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
            type: "text",
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
            key: "filter",
            label_en: "Filter ($filter)",
            label_de: "Filter ($filter)",
            type: "text",
            desc_en: "Optional OData $filter expression to fetch specific accounts"
        },
        {
            key: "extends",
            label_en: "Extends ($expand)",
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
    tags: ["reader", "api", "sap", "c4c", "accounts"],
    hideInToolbox: true,
    tests: () => {}
})


// Export all tools using the standard pattern
tools.exportAll(exports)
