# Chioro Reader Plugin Development Guide for AI Agents

This document provides comprehensive instructions for AI agents to create custom reader plugins for the Chioro data integration platform. Reader plugins allow Chioro to import data from various sources (files, APIs, databases, etc.) into its processing pipeline.

## Overview

A **reader plugin** is a JavaScript module that:
1. Reads data from a source (file stream or external API)
2. Parses the data into individual records (JavaScript objects)
3. Yields records one at a time to Chioro's processing pipeline

## Plugin Architecture

### File Structure

```
my-reader-plugin/
├── package.json       # NPM package definition
├── index.js           # Main plugin code
├── test.js            # Test runner
└── README.md          # Documentation
```

### package.json Template

```json
{
  "name": "chioro-my-reader",
  "version": "1.0.0",
  "description": "Custom reader plugin for Chioro",
  "main": "index.js",
  "scripts": {
    "test": "node test.js"
  },
  "dependencies": {
    "chioro-toolbox": "github:eCubeGmbH/chioro-toolbox"
  }
}
```

## Reader Plugin Function Signature

Every reader plugin must export a function with this signature:

```javascript
function myReaderPlugin(config, streamHelper, journal) {
    // Returns a reader object
    return {
        open: function() { /* ... */ },
        readRecords: function*() { /* generator */ },
        close: function() { /* ... */ }
    };
}
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | Object | Configuration values from the UI as a plain JavaScript object. Access via `config.key` or `config["key"]` |
| `streamHelper` | Object | Helper for reading file input streams (for file-based readers) |
| `journal` | Object | Progress reporting. Call `journal.onProgress(count)` to report progress |

### Return Object: The Reader Interface

The function must return an object with exactly these three methods:

#### `open()`
- Called once when the reader starts
- Initialize resources, open connections, read file headers
- For file readers: call `streamHelper.open(encoding)`
- For API readers: fetch initial data

#### `readRecords()` (Generator Function)
- **MUST be a generator function** (using `function*` syntax)
- Yields records one at a time using `yield record`
- Each record should be a plain JavaScript object
- Called repeatedly until no more records

#### `close()`
- Called when reading is complete (success or error)
- Clean up resources, close streams/connections
- Reset internal state

## Configuration Access

The `config` parameter is a plain JavaScript object (parsed from JSON at the Java/JS boundary). You can access values directly with `config.key` or use the global `getConfigValue` helper function for default value handling.

## JavaScript Runtime Environment

**IMPORTANT: Chioro runs JavaScript inside GraalVM, which is a pure ECMAScript 2020 implementation. Only standard ECMAScript features and Chioro-provided functions are available.**

- ✅ ECMAScript 2020 features (arrow functions, destructuring, spread, async/await, etc.)
- ✅ Chioro-provided global functions (documented below)
- ❌ Browser APIs (`btoa`, `atob`, `fetch`, `XMLHttpRequest`, `document`, `window`, `setTimeout`)
- ❌ Node.js APIs (`Buffer`, `require('fs')`, `require('http')`, `process`, `__dirname`)

## Available Global Functions

Only use functions documented here. Chioro injects these into the GraalVM context:

### HTTP Functions (from chioro-toolbox)

```javascript
// GET request returning parsed JSON
var data = getJson(url, headers);

// POST request returning parsed JSON
var data = postJson(url, body, headers);

// PUT request returning parsed JSON
var data = putJson(url, body, headers);

// GET request returning raw text
var text = getText(url, headers);
```

Example:
```javascript
var headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer " + token
};
var data = getJson("https://api.example.com/items", headers);
```

### Low-Level HTTP Access via `_apiFetcher`

For cases where `getJson`/`postJson`/`putJson` are insufficient (e.g., DELETE requests, custom response handling, or raw string responses), use `_apiFetcher` directly:

```javascript
// GET - returns raw string (not parsed JSON)
var response = _apiFetcher.getUrl(url);
var response = _apiFetcher.getUrl(url, headers);

// POST - body must be a JSON string, returns raw string
var response = _apiFetcher.postUrl(url, JSON.stringify(body));
var response = _apiFetcher.postUrl(url, JSON.stringify(body), headers);

// PUT - body must be a JSON string, returns void
_apiFetcher.putUrl(url, JSON.stringify(body), headers);

// DELETE
_apiFetcher.deleteUrl(url, headers);

// HEAD - returns headers as JSON string
var headersJson = _apiFetcher.headUrl(url, true);  // true = follow redirects
```

Example with DELETE:
```javascript
var headers = {
    "Authorization": "Basic " + base64Encode(username + ":" + password)
};
_apiFetcher.deleteUrl("https://api.example.com/items/123", headers);
```

Example parsing raw response:
```javascript
var rawResponse = _apiFetcher.getUrl(url, headers);
var data = JSON.parse(rawResponse);
```

### Utility Functions (from Java via ChioroScriptUtils)

These functions are injected from Java and available globally:

```javascript
// Base64 encoding (for HTTP Basic Auth)
var encoded = base64Encode("user:password");  // "dXNlcjpwYXNzd29yZA=="

// Base64 decoding
var decoded = base64Decode("dXNlcjpwYXNzd29yZA==");  // "user:password"

// Sleep for API rate limiting (milliseconds)
sleep(1000);  // Wait 1 second between API calls

// Get config value safely (works with Java Map or JS object)
var value = getConfigValue(config, "key", "defaultValue");
```

### getConfigValue Helper

The `getConfigValue` function is globally available for safely accessing config values with defaults:

```javascript
// Signature
getConfigValue(config, key, defaultValue)

// Examples
var baseUrl = getConfigValue(config, 'baseUrl', 'http://localhost');
var pageSize = getConfigValue(config, 'pageSize', 100);
var authConfig = getConfigValue(config, 'authConfig', null);

// Or use direct property access (config is always a plain JS object)
var baseUrl = config.baseUrl || 'http://localhost';
var token = config.authConfig?.properties?.bearerToken || '';
```

The `config` parameter is always a plain JavaScript object (serialized to JSON at the Java boundary), so direct property access works reliably.

Example with API throttling:
```javascript
var page = 1;
var hasMore = true;
while (hasMore) {
    var data = getJson(baseUrl + "?page=" + page, headers);
    // process data...
    page++;
    hasMore = data.hasNextPage;

    if (hasMore) {
        sleep(500);  // Wait 500ms before next request to avoid rate limiting
    }
}
```

### Standard Library Functions

All functions from `chioro-toolbox/toolbase` are available with the `base.` prefix:
```javascript
var upper = base.upperCaseText("hello");  // "HELLO"
var lower = base.lowerCaseText("WORLD");  // "world"
```


## StreamHelper API (For File-Based Readers)

When reading from files uploaded to Chioro:

```javascript
// Open stream with encoding
streamHelper.open("UTF-8");

// Read one line at a time
var line = streamHelper.readLine();  // Returns null at EOF

// Check if stream is open
var isOpen = streamHelper.isOpen();

// Close the stream
streamHelper.close();
```

## Complete Examples

### Example 1: File-Based Reader (Pipe-Separated)

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("My File Reader");

// Note: getConfigValue is globally available - no need to define it

function pipeReaderPlugin(config, streamHelper, journal) {
    var allRecords = [];
    var currentIndex = 0;
    var headers = [];

    return {
        open: function() {
            var encoding = getConfigValue(config, "encoding", "UTF-8");
            streamHelper.open(encoding);

            var lines = [];
            var line;
            while ((line = streamHelper.readLine()) !== null) {
                if (line.trim() !== '') {
                    lines.push(line);
                }
            }

            if (lines.length === 0) return;

            // First line is header
            headers = lines[0].split('|').map(function(h) {
                return h.trim();
            });

            // Parse data rows
            for (var i = 1; i < lines.length; i++) {
                var values = lines[i].split('|');
                var record = {};
                for (var j = 0; j < headers.length; j++) {
                    record[headers[j]] = (values[j] || '').trim();
                }
                allRecords.push(record);
            }

            journal.onProgress(allRecords.length);
        },

        readRecords: function*() {
            while (currentIndex < allRecords.length) {
                yield allRecords[currentIndex++];
            }
        },

        close: function() {
            if (streamHelper && streamHelper.isOpen()) {
                streamHelper.close();
            }
            allRecords = [];
            headers = [];
            currentIndex = 0;
        }
    };
}

tools.add({
    id: "pipeReaderPlugin",
    impl: pipeReaderPlugin,
    aliases: { en: "pipeReaderPlugin", de: "pipeReaderPlugin" },
    simpleDescription: {
        en: "Reads pipe-separated files",
        de: "Liest Pipe-getrennte Dateien"
    },
    args: [
        {
            key: "encoding",
            label_en: "Encoding",
            label_de: "Kodierung",
            type: "select",
            options: ["UTF-8", "ISO-8859-1", "Windows-1252"],
            default: "UTF-8"
        }
    ],
    tags: ["reader", "file"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

### Example 2: API-Based Reader (with Pagination and AdminConfig Auth)

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("API Reader Plugin");

// Note: getConfigValue is globally available - no need to define it

/**
 * Extract authentication from resolved AdminConfig
 */
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

function apiReaderPlugin(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', '');
    var endpoint = getConfigValue(config, 'endpoint', '/api/items');
    var pageSize = getConfigValue(config, 'pageSize', 100);

    // Get authentication from AdminConfig
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);

    var allRecords = [];
    var currentIndex = 0;

    return {
        open: function() {
            var headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            };

            // Apply authentication based on AdminConfig type
            if (auth.type === 'bearer' && auth.token) {
                headers["Authorization"] = "Bearer " + auth.token;
            } else if (auth.type === 'basic' && auth.username && auth.password) {
                headers["Authorization"] = "Basic " + base64Encode(auth.username + ':' + auth.password);
            }

            // Paginated fetch
            var page = 1;
            var hasMore = true;

            while (hasMore) {
                var url = baseUrl + endpoint + "?page=" + page + "&page_size=" + pageSize;
                var data = getJson(url, headers);

                if (data && data.data && data.data.length > 0) {
                    for (var i = 0; i < data.data.length; i++) {
                        allRecords.push(data.data[i]);
                    }
                    journal.onProgress(allRecords.length);
                    page++;
                    // Use pagination info from response
                    hasMore = data.pagination && data.pagination.has_next;
                } else {
                    hasMore = false;
                }
            }
        },

        readRecords: function*() {
            while (currentIndex < allRecords.length) {
                yield allRecords[currentIndex++];
            }
        },

        close: function() {
            allRecords = [];
            currentIndex = 0;
        }
    };
}

tools.add({
    id: "apiReaderPlugin",
    impl: apiReaderPlugin,
    aliases: { en: "apiReaderPlugin", de: "apiReaderPlugin" },
    simpleDescription: {
        en: "API Reader - fetches data from REST APIs",
        de: "API Reader - holt Daten von REST APIs"
    },
    args: [
        {
            key: "baseUrl",
            label_en: "API Base URL",
            label_de: "API Basis-URL",
            type: "text",
            required: true,
            desc_en: "Base URL of the API (e.g., http://localhost:8089)",
            desc_de: "Basis-URL der API (z.B. http://localhost:8089)"
        },
        {
            key: "endpoint",
            label_en: "Endpoint",
            label_de: "Endpunkt",
            type: "text",
            default: "/api/documents",
            desc_en: "API endpoint path",
            desc_de: "API-Endpunktpfad"
        },
        {
            key: "pageSize",
            label_en: "Page Size",
            label_de: "Seitengröße",
            type: "text",
            default: "100",
            desc_en: "Number of records per page",
            desc_de: "Anzahl der Datensätze pro Seite"
        },
        {
            key: "authConfig",
            label_en: "Authentication",
            label_de: "Authentifizierung",
            type: "adminconfig",
            subType: "BASIC_AUTH",
            required: true,
            desc_en: "Select Basic Auth credentials from AdminConfig",
            desc_de: "Basic Auth Anmeldedaten aus AdminConfig auswählen"
        }
    ],
    tags: ["reader", "api"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

## Configuration Schema (args)

The `args` array defines the configuration UI. Each arg object:

| Property | Type | Description |
|----------|------|-------------|
| `key` | string | Config key name (used in `config.get(key)`) |
| `label_en` | string | English label for UI |
| `label_de` | string | German label for UI |
| `type` | string | One of: `text`, `boolean`, `select`, `adminconfig` |
| `subType` | string | For `adminconfig` type: filter by AdminConfig subType |
| `options` | array | For `select` type: list of options |
| `default` | any | Default value |
| `required` | boolean | Whether field is required |
| `desc_en` | string | English description (always include) |
| `desc_de` | string | German description (always include - app default language is German) |

## AdminConfig Type for Secure Credentials

For API readers that need authentication, use the `adminconfig` type to reference tenant-level secrets stored securely in Chioro. This is the **recommended approach** for handling API credentials.

### Available AdminConfig SubTypes for API Authentication

| SubType | Description | Properties |
|---------|-------------|------------|
| `BEARER_TOKEN` | Bearer token authentication | `bearerToken` |
| `BASIC_AUTH` | Basic HTTP authentication | `basicAuthUsername`, `basicAuthPassword` |

### Declaring AdminConfig Args

```javascript
args: [
    {
        key: "authConfig",
        label_en: "Authentication",
        label_de: "Authentifizierung",
        type: "adminconfig",
        subType: "BASIC_AUTH",  // Shows only BASIC_AUTH configs in dropdown
        required: true,
        desc_en: "Select the authentication configuration"
    }
]
```

### Extracting Credentials from AdminConfig

When Chioro resolves the config, `adminconfig` fields contain the full AdminConfig object. Use this helper to extract credentials (note: `getConfigValue` is globally available):

```javascript
/**
 * Extract authentication credentials from a resolved AdminConfig
 * @param {Object} authConfig - The resolved AdminConfig object from config
 * @returns {Object} Auth object with type, token, username, password
 */
function getAuthFromAdminConfig(authConfig) {
    if (!authConfig) {
        return { type: 'none', token: '', username: '', password: '' };
    }

    // getConfigValue is globally available
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
```

### Using Authentication in HTTP Requests

```javascript
var authConfig = getConfigValue(config, 'authConfig', null);
var auth = getAuthFromAdminConfig(authConfig);

var headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
};

if (auth.type === 'bearer' && auth.token) {
    headers["Authorization"] = "Bearer " + auth.token;
} else if (auth.type === 'basic' && auth.username && auth.password) {
    headers["Authorization"] = "Basic " + base64Encode(auth.username + ':' + auth.password);
}
```

### Base64 Encoding for Basic Auth

Chioro provides global `base64Encode` and `base64Decode` functions (injected from Java via `ChioroScriptUtils`):

```javascript
// Encode string to Base64
var credentials = base64Encode(username + ":" + password);
headers["Authorization"] = "Basic " + credentials;

// Decode Base64 string (if needed)
var decoded = base64Decode("dXNlcjpwYXNzd29yZA==");  // "user:password"
```

**Note:** These functions are automatically available in all Chioro script contexts. They use Java's `java.util.Base64` under the hood.

## Registration in tools.add()

| Property | Required | Description |
|----------|----------|-------------|
| `id` | Yes | Unique identifier (should match function name) |
| `impl` | Yes | Reference to the function |
| `aliases` | Yes | Object with `en` and `de` names |
| `simpleDescription` | No | Short description in `en` and `de` |
| `args` | No | Configuration schema array |
| `tags` | No | Array of tags (include "reader") |
| `hideInToolbox` | No | Set to `true` for reader plugins |
| `tests` | No | Test function |

## Testing Locally

Create a `test.js` file:

```javascript
#!/usr/bin/env node

// Mock the globals that Chioro provides
global.getJson = function(url, headers) {
    console.log("Mock getJson called:", url);
    return { items: [{id: 1, name: "Test"}], hasMore: false };
};

const mod = require('./index');
mod.tools.testAllTools();
mod.tools.stats();
```

Run with: `npm test`

## Setting Up AdminConfig for API Authentication

Before using a reader plugin that requires authentication, create an AdminConfig entry:

1. In Chioro, go to **Admin > Admin Configs**
2. Click **Add** to create a new configuration
3. Fill in the required fields:
   - **Name**: Descriptive name (e.g., "My API Credentials")
   - **SubType**: Select the authentication type:
     - `BASIC_AUTH` for username/password authentication
     - `BEARER_TOKEN` for token-based authentication
4. In the **Properties** section, add the credentials:

   For `BASIC_AUTH`:
   - `basicAuthUsername`: The API username
   - `basicAuthPassword`: The API password

   For `BEARER_TOKEN`:
   - `bearerToken`: The API token

5. Save the AdminConfig

The AdminConfig will now appear in the dropdown when configuring reader plugins that use `type: "adminconfig"` with the matching `subType`.

## Registering in Chioro UI

After publishing your plugin to GitHub:

1. In Chioro, go to **Admin > Script Libraries**
2. Add a new library with:
   - **Name**: A friendly name
   - **Alias**: Short identifier (e.g., `my-reader`)
   - **URL**: GitHub URL (e.g., `github:myorg/chioro-my-reader`)
3. Go to a **Data Source** configuration
4. Click the **+** button next to the Format dropdown
5. Register your plugin:
   - **Plugin ID**: Unique format identifier (e.g., `MY_READER`)
   - **Library Alias**: The alias from step 2
   - **Function Name**: Your function name (e.g., `myReaderPlugin`)
   - **Display Name**: Human-readable name

## Best Practices

1. **Always use generator functions** for `readRecords` - Chioro expects this pattern
2. **Report progress** using `journal.onProgress(count)` for large datasets
3. **Handle errors gracefully** - wrap risky operations in try/catch
4. **Clean up in close()** - always reset state and close resources
5. **Use `hideInToolbox: true`** for reader plugins (they're not transformation tools)
6. **Include the "reader" tag** for discoverability
7. **Use direct property access** - config is always a plain JS object (`config.key` or `getConfigValue(config, 'key', default)`)
8. **Test with mock data** before deploying
9. **Use AdminConfig for credentials** - never hardcode secrets; use `type: "adminconfig"` with appropriate `subType`
10. **Use pagination response metadata** - check `has_next`, `total_pages`, or similar fields to know when to stop fetching

## Common Patterns

### Reading All Data First, Then Yielding

```javascript
var allRecords = [];
var currentIndex = 0;

return {
    open: function() {
        // Fetch all data into allRecords array
    },
    readRecords: function*() {
        while (currentIndex < allRecords.length) {
            yield allRecords[currentIndex++];
        }
    },
    close: function() {
        allRecords = [];
        currentIndex = 0;
    }
};
```

### Streaming Records (Memory Efficient)

```javascript
return {
    open: function() {
        streamHelper.open("UTF-8");
        // Read header line
    },
    readRecords: function*() {
        var line;
        while ((line = streamHelper.readLine()) !== null) {
            yield parseLine(line);
        }
    },
    close: function() {
        streamHelper.close();
    }
};
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Config values are undefined | Use `getConfigValue(config, key, default)` or check `config.key !== undefined` |
| Records not appearing | Ensure `readRecords` is a generator (`function*`) and uses `yield` |
| Stream errors | Always call `streamHelper.open()` before reading |
| Progress not showing | Call `journal.onProgress(count)` during `open()` |
| Plugin not found | Verify the function is exported via `tools.exportAll(exports)` |
| AdminConfig dropdown empty | Ensure AdminConfig exists with matching `subType` |
| Auth credentials are null | Use `getAuthFromAdminConfig` helper to extract from `properties` |
| 401 Unauthorized | Check AdminConfig has correct credentials; verify `subType` matches |

## Summary

To create a reader plugin:

1. Create a function that returns `{ open, readRecords*, close }`
2. Register it with `tools.add({ ... })`
3. Export with `tools.exportAll(exports)`
4. Publish to GitHub

---

# Writer Plugin Development

Writer plugins export data from Chioro to external destinations (APIs, files, etc.). They follow a similar pattern to readers but with a different interface.

## Writer Function Signature

```javascript
function myWriterPlugin(config, streamHelper, journal) {
    return {
        open: function() { /* initialize */ },
        writeRecord: function(record) { /* write single record */ },
        close: function() { /* cleanup */ }
    };
}
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | Object | Configuration values as a plain JavaScript object. Access via `config.key` or `config["key"]` |
| `streamHelper` | Object | Helper for writing to file output streams (for file-based writers) |
| `journal` | Object | Progress reporting via `journal.onProgress(count)` |

### Return Object: The Writer Interface

#### `open()`
- Called once when the writer starts
- Initialize connections, authenticate to APIs

#### `writeRecord(recordJson)`
- Called once per record (NOT a generator)
- `recordJson` is a **JSON string** - you must parse it: `var record = JSON.parse(recordJson)`
- After parsing, `record` is a plain JavaScript object with the record data
- Send/write the record to destination

#### `close()`
- Called when writing is complete
- Flush buffers, close connections

## Writer Example: API Writer

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("API Writer Plugin");

// Note: getConfigValue is globally available - no need to define it

function apiWriterPlugin(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', '');
    var endpoint = getConfigValue(config, 'endpoint', '/api/documents');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var headers = {};
    var recordCount = 0;

    return {
        open: function() {
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            };

            // Apply authentication from AdminConfig
            var auth = getAuthFromAdminConfig(authConfig);
            if (auth.type === 'bearer' && auth.token) {
                headers["Authorization"] = "Bearer " + auth.token;
            } else if (auth.type === 'basic' && auth.username && auth.password) {
                headers["Authorization"] = "Basic " + base64Encode(auth.username + ':' + auth.password);
            }
        },

        writeRecord: function(record) {
            var url = baseUrl + endpoint;
            var recordId = record.id || record.ID || null;

            // Use PUT for updates (if record has ID), POST for creates
            if (recordId) {
                url = baseUrl + endpoint + '/' + recordId;
                putJson(url, record, headers);
            } else {
                postJson(url, record, headers);
            }

            recordCount++;
            journal.onProgress(recordCount);
        },

        close: function() {
            recordCount = 0;
        }
    };
}

tools.add({
    id: "apiWriterPlugin",
    impl: apiWriterPlugin,
    aliases: { en: "apiWriterPlugin", de: "apiWriterPlugin" },
    simpleDescription: {
        en: "API Writer - sends data to REST APIs",
        de: "API Writer - sendet Daten an REST APIs"
    },
    args: [
        { key: "baseUrl", label_en: "API Base URL", label_de: "API Basis-URL", type: "text", required: true, desc_en: "Base URL of the API", desc_de: "Basis-URL der API" },
        { key: "endpoint", label_en: "Endpoint", label_de: "Endpunkt", type: "text", default: "/api/documents", desc_en: "API endpoint path", desc_de: "API-Endpunktpfad" },
        { key: "authConfig", label_en: "Authentication", label_de: "Authentifizierung", type: "adminconfig", subType: "BASIC_AUTH", required: true, desc_en: "Select authentication", desc_de: "Authentifizierung auswählen" }
    ],
    tags: ["writer", "api"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

## StreamHelper API (For File-Based Writers)

When writing to files, use `streamHelper`:

```javascript
// Open stream with encoding
streamHelper.open("UTF-8");

// Write raw content (no newline)
streamHelper.write("content");

// Write a line (appends newline)
streamHelper.writeLine("line content");

// Flush the buffer
streamHelper.flush();

// Close the stream
streamHelper.close();
```

## File-Based Writer Example: Pipe-Separated Writer

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("Pipe Writer Plugin");

function pipeWriterPlugin(config, streamHelper, journal) {
    var recordCount = 0;
    var headers = null;

    return {
        open: function() {
            streamHelper.open("UTF-8");
            recordCount = 0;
            headers = null;
        },

        writeRecord: function(record) {
            // Convert record to plain object
            var recordObj = {};
            if (record && typeof record.get === 'function' && typeof record.keysCount === 'function') {
                var count = record.keysCount();
                for (var i = 0; i < count; i++) {
                    var key = record.keyAt(i);
                    recordObj[key] = record.get(key);
                }
            } else if (record && typeof record === 'object') {
                for (var key in record) {
                    if (record.hasOwnProperty(key)) {
                        recordObj[key] = record[key];
                    }
                }
            }

            // Write header on first record
            if (headers === null) {
                headers = Object.keys(recordObj);
                streamHelper.writeLine(headers.join('|'));
            }

            // Write values
            var values = headers.map(function(h) {
                var val = recordObj[h];
                return (val !== null && val !== undefined) ? String(val) : '';
            });
            streamHelper.writeLine(values.join('|'));

            recordCount++;
            journal.onProgress(recordCount);
        },

        close: function() {
            streamHelper.flush();
            streamHelper.close();
        }
    };
}

tools.add({
    id: "pipeWriterPlugin",
    impl: pipeWriterPlugin,
    aliases: { en: "pipeWriterPlugin", de: "pipeWriterPlugin" },
    simpleDescription: {
        en: "Writes records as pipe-separated file",
        de: "Schreibt Datensätze als Pipe-getrennte Datei"
    },
    args: [],
    tags: ["writer", "file"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

## HTTP Functions for Writers

```javascript
// POST request (create)
var response = postJson(url, body, headers);

// PUT request (update)
var response = putJson(url, body, headers);
```

## Key Differences from Readers

| Aspect | Reader | Writer |
|--------|--------|--------|
| Record method | `readRecords: function*()` (generator, yields) | `writeRecord: function(record)` (regular function) |
| Data flow | Source → Chioro | Chioro → Destination |
| Tags | `["reader"]` | `["writer"]` |
