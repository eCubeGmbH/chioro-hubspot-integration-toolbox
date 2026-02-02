/**
 * Pipe-separated file reader plugin for Chioro.
 *
 * This is a simple reader that parses pipe-separated (|) files.
 * It demonstrates how to create a custom reader plugin.
 */

const Toolpackage = require('chioro-toolbox/toolpackage')

const tools = new Toolpackage("Pipe Reader Tools")
tools.description = 'Plugin for reading pipe-separated files'

function getConfigValue(config, key, defaultValue) {
    if (!config) return defaultValue;
    var value = typeof config.get === 'function' ? config.get(key) : config[key];
    return (value !== undefined && value !== null) ? value : defaultValue;
}

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
 * Pipe reader plugin function.
 *
 * @param {Object} config - Configuration object with reader settings
 * @param {Object} streamHelper - Helper for reading input stream
 * @param {Object} journal - Journal for progress reporting
 * @returns {Object} Reader object with open, readRecords, and close methods
 */
function pipeReaderPlugin(config, streamHelper, journal) {
    var headers = [];
    var hasHeader = true;
    var recordCount = 0;

    return {
        /**
         * Opens the reader and prepares for reading.
         * Only reads the header line if hasHeader is true.
         */
        open: function() {
            // Open the stream with the configured encoding
            var encoding = config.get("encoding") || "UTF-8";
            streamHelper.open(encoding);

            // Check if first row contains headers
            hasHeader = config.has("hasHeader") ? config.get("hasHeader") : true;

            if (hasHeader) {
                // Read and parse the header line
                var headerLine = streamHelper.readLine();
                if (headerLine !== null && headerLine.trim() !== '') {
                    headers = headerLine.split('|').map(function(h) {
                        return h.trim();
                    });
                }
            }
            // If no header, we'll determine columns from the first data line in readRecords
        },

        /**
         * Generator that reads and yields records line by line.
         */
        readRecords: function*() {
            var line;
            var isFirstDataLine = true;

            while ((line = streamHelper.readLine()) !== null) {
                // Skip empty lines
                if (line.trim() === '') {
                    continue;
                }

                var values = line.split('|').map(function(v) {
                    return v.trim();
                });

                // If no header, generate column names from first data line
                if (!hasHeader && isFirstDataLine) {
                    headers = values.map(function(_, i) {
                        return 'col' + String(i + 1).padStart(3, '0');
                    });
                }
                isFirstDataLine = false;

                // Create record object
                var record = {};
                for (var j = 0; j < headers.length; j++) {
                    record[headers[j]] = values[j] || '';
                }

                recordCount++;

                // Report progress (current record count)
                if (journal && journal.onProgress) {
                    journal.onProgress(recordCount);
                }

                yield record;
            }
        },

        /**
         * Closes the reader and cleans up.
         */
        close: function() {
            if (streamHelper && streamHelper.isOpen()) {
                streamHelper.close();
            }
            headers = [];
            recordCount = 0;
        }
    };
}

tools.add({
    id: "pipeReaderPlugin",
    impl: pipeReaderPlugin,
    aliases: {
        en: "pipeReaderPlugin",
        de: "pipeReaderPlugin"
    },
    simpleDescription: {
        en: "Reads pipe-separated (|) files",
        de: "Liest Pipe-getrennte (|) Dateien"
    },
    args: [
        {
            key: "encoding",
            label_en: "Encoding",
            label_de: "Kodierung",
            type: "select",
            options: ["UTF-8", "ISO-8859-1", "Windows-1252"],
            default: "UTF-8"
        },
        {
            key: "hasHeader",
            label_en: "First Row is Header",
            label_de: "Erste Zeile ist Kopfzeile",
            type: "boolean",
            default: true
        }
    ],
    tags: ["reader", "file", "pipe"],
    hideInToolbox: false,
    tests: () => {}
})

/**
 * API reader plugin for paginated documents endpoint.
 *
 * @param {Object} config - Configuration object with reader settings
 * @param {Object} streamHelper - Unused for API readers
 * @param {Object} journal - Journal for progress reporting
 * @returns {Object} Reader object with open, readRecords, and close methods
 */
function testApiReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://2c7e6ca2e721.ngrok-free.app');
    var endpoint = getConfigValue(config, 'endpoint', '/api/documents');
    var pageSize = parseInt(getConfigValue(config, 'pageSize', 100), 10);
    if (!pageSize || pageSize < 1) {
        pageSize = 100;
    }

    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);

    var page = 1;
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

        if (auth.type === 'bearer' && auth.token) {
            hdrs["Authorization"] = "Bearer " + auth.token;
        } else if (auth.type === 'basic' && auth.username && auth.password) {
            hdrs["Authorization"] = "Basic " + base64Encode(auth.username + ":" + auth.password);
        }

        return hdrs;
    }

    function fetchNextPage() {
        if (!hasMore) return;

        var url = baseUrl + endpoint + "?page=" + page + "&page_size=" + pageSize;
        var data = getJson(url, headers);

        buffer = (data && data.data && data.data.length) ? data.data : [];
        bufferIndex = 0;

        if (data && data.pagination) {
            if (typeof data.pagination.has_next === 'boolean') {
                hasMore = data.pagination.has_next;
            } else if (typeof data.pagination.total_pages === 'number') {
                hasMore = page < data.pagination.total_pages;
            } else {
                hasMore = buffer.length === pageSize;
            }
        } else {
            hasMore = buffer.length === pageSize;
        }

        if (buffer.length > 0 || hasMore) {
            page++;
        }
    }

    return {
        open: function() {
            page = 1;
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
            page = 1;
            hasMore = true;
            buffer = [];
            bufferIndex = 0;
            recordCount = 0;
            headers = null;
        }
    };
}

tools.add({
    id: "testApiReader",
    impl: testApiReader,
    aliases: {
        en: "testApiReader",
        de: "testApiReader"
    },
    simpleDescription: {
        en: "Reads documents from a paginated REST API",
        de: "Liest Dokumente von einer paginierten REST API"
    },
    args: [
        {
            key: "baseUrl",
            label_en: "API Base URL",
            label_de: "API Basis-URL",
            type: "text",
            required: true,
            default: "https://2c7e6ca2e721.ngrok-free.app",
            desc_en: "Base URL of the API"
        },
        {
            key: "endpoint",
            label_en: "Endpoint",
            label_de: "Endpunkt",
            type: "text",
            default: "/api/documents",
            desc_en: "API endpoint path"
        },
        {
            key: "pageSize",
            label_en: "Page Size",
            label_de: "Seitengröße",
            type: "text",
            default: "100",
            desc_en: "Number of records per page"
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
    tags: ["reader", "api", "documents"],
    hideInToolbox: true,
    tests: () => {}
})

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
        query.push("$sap-label=true");

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
