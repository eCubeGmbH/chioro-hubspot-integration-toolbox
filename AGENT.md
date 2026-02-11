# Chioro Plugin Development Guide for AI Agents

This document provides instructions for AI agents to create custom reader and writer plugins for the Chioro data integration platform.

## Important Notes for AI Agents

1. **Do NOT write tests** - The local test infrastructure does not work reliably because it cannot properly mock the GraalVM runtime environment and Chioro-injected globals. Writing tests wastes tokens without providing value.

2. **You cannot deploy or verify plugins** - The user must manually deploy plugins to Chioro and test them. Your job is only to write the plugin code.

3. **Focus on writing correct code** - Since you cannot test, ensure your code follows the patterns in this document exactly.

4. **Don't install the dependencies or run npm** - There is no point to it.

5. **All code must be in or included from the main file** - The plugin entry point is defined by the `main` field in `package.json` (typically `index.js`). Therefore:
   - Keep the metadata (`tools.add()`, `args`, `tools.exportAll()`) in the main file and put the plugin logic in a separate file that you `require()` in the main file
   **WARNING:** Creating separate `.js` files without requiring them in the main file means that code will NOT be loaded. Chioro only loads the file specified in `package.json`'s `main` field.

---

# Part 1: Common Concepts (Readers & Writers)

## Plugin Architecture

### File Structure

```
my-plugin/
├── package.json       # NPM package definition (defines main entry point)
├── index.js           # Main plugin code - ALL code must be here or required from here
└── README.md          # Documentation
```

**CRITICAL:** Chioro only loads the file specified in `package.json`'s `main` field. Either:
- Write all code in `index.js`, OR
- Keep the metadata (`tools.add()`, `args`, `tools.exportAll()`) in `index.js` and require the plugin logic:
```javascript
// index.js
const myPlugin = require('./my-plugin-logic.js');
tools.add({ id: "myPlugin", impl: myPlugin, tags: ["dynamic-plugin", "reader"], ... });
```

### package.json Template

```json
{
  "name": "chioro-my-plugin",
  "version": "1.0.0",
  "description": "Custom plugin for Chioro",
  "main": "index.js",
  "dependencies": {
    "chioro-toolbox": "github:eCubeGmbH/chioro-toolbox"
  }
}
```

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

For cases where `getJson`/`postJson`/`putJson` are insufficient (e.g., PATCH, DELETE requests, custom response handling, or raw string responses), use `_apiFetcher` directly:

```javascript
// GET - returns raw string (not parsed JSON)
var response = _apiFetcher.getUrl(url);
var response = _apiFetcher.getUrl(url, headers);

// POST - body must be a JSON string, returns raw string
var response = _apiFetcher.postUrl(url, JSON.stringify(body));
var response = _apiFetcher.postUrl(url, JSON.stringify(body), headers);

// PUT - body must be a JSON string, returns void
_apiFetcher.putUrl(url, JSON.stringify(body), headers);

// PATCH - body must be a JSON string, returns raw string response
var response = _apiFetcher.patchUrl(url, JSON.stringify(body), headers);

// DELETE
_apiFetcher.deleteUrl(url, headers);

// HEAD - returns headers as JSON string
var headersJson = _apiFetcher.headUrl(url, true);  // true = follow redirects
```

### Utility Functions (from Java via ChioroScriptUtils)

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

```javascript
// Signature
getConfigValue(config, key, defaultValue)

// Examples
var baseUrl = getConfigValue(config, 'baseUrl', 'http://localhost');
var pageSize = getConfigValue(config, 'pageSize', 100);

// Or use direct property access (config is always a plain JS object)
var baseUrl = config.baseUrl || 'http://localhost';
var token = config.authConfig?.properties?.bearerToken || '';
```

### Standard Library Functions

All functions from `chioro-toolbox/toolbase` are available with the `base.` prefix:
```javascript
var upper = base.upperCaseText("hello");  // "HELLO"
var lower = base.lowerCaseText("WORLD");  // "world"
```

## Configuration Schema (args)

The `args` array defines the configuration UI. Each arg object:

| Property | Type | Description |
|----------|------|-------------|
| `key` | string | Config key name (used in `config.key`) |
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

For plugins that need authentication, use the `adminconfig` type to reference tenant-level secrets stored securely in Chioro.

### Available AdminConfig SubTypes

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
        desc_en: "Select the authentication configuration",
        desc_de: "Authentifizierungskonfiguration auswählen"
    }
]
```

### Extracting Credentials from AdminConfig

```javascript
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

## Registration in tools.add()

| Property | Required | Description |
|----------|----------|-------------|
| `id` | Yes | Unique identifier (should match function name) |
| `impl` | Yes | Reference to the function |
| `aliases` | Yes | Object with `en` and `de` names |
| `simpleDescription` | No | Short description in `en` and `de` |
| `args` | No | Configuration schema array |
| `tags` | No | Array of tags — used for grouping and for auto-registration (see below) |
| `hideInToolbox` | No | Set to `true` for reader/writer plugins |

### Auto-Registration via Tags

The Chioro backend automatically registers scripts as dynamic reader/writer plugins based on their **tags** when a library is loaded. No manual REST API registration is needed.

**Required tags for auto-registration:**
- `"dynamic-plugin"` — marks this script for auto-registration as a dynamic plugin
- `"reader"` or `"writer"` — determines the plugin type

**Optional tag:**
- `"file-based"` — if present, the plugin expects file input/output. If absent, the plugin is API-based (no file stream)

**Behavior:**
- If a plugin with the same ID is already manually registered, auto-registration is skipped (manual registrations take precedence)
- These tags are optional — existing tools without them continue to work unchanged
- Tools without the `"dynamic-plugin"` tag are never auto-registered

**Example tags for an API-based reader:**
```javascript
tags: ["dynamic-plugin", "reader"]
```

**Example tags for a file-based writer:**
```javascript
tags: ["dynamic-plugin", "writer", "file-based"]
```

## Best Practices (All Plugins)

1. **Report progress** using `journal.onProgress(count)` for large datasets
2. **Handle errors gracefully** - wrap risky operations in try/catch
3. **Clean up in close()** - always reset state and close resources
4. **Use `hideInToolbox: true`** - reader/writer plugins are not transformation tools
5. **Use direct property access** - config is always a plain JS object (`config.key` or `getConfigValue(config, 'key', default)`)
6. **Use AdminConfig for credentials** - never hardcode secrets
7. **Use pagination response metadata** - check `has_next`, `total_pages`, or similar fields

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Config values are undefined | Use `getConfigValue(config, key, default)` or check `config.key !== undefined` |
| Plugin not found | Verify the function is exported via `tools.exportAll(exports)` |
| AdminConfig dropdown empty | Ensure AdminConfig exists with matching `subType` |
| Auth credentials are null | Use `getAuthFromAdminConfig` helper to extract from `properties` |
| 401 Unauthorized | Check AdminConfig has correct credentials; verify `subType` matches |

---

# Part 2: Reader Plugins

Reader plugins import data from external sources (files, APIs, databases) into Chioro's processing pipeline.

## Reader Function Signature

```javascript
function myReaderPlugin(config, streamHelper, journal) {
    return {
        open: function() { /* initialize */ },
        readRecords: function*() { /* generator - yields records */ },
        close: function() { /* cleanup */ }
    };
}
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | Object | Configuration values from the UI |
| `streamHelper` | Object | Helper for reading file input streams (for file-based readers) |
| `journal` | Object | Progress reporting via `journal.onProgress(count)` |

### Reader Interface Methods

#### `open()`
- Called once when the reader starts
- Initialize resources, open connections, read file headers
- For file readers: call `streamHelper.open(encoding)`
- For API readers: fetch initial data

#### `readRecords()` (Generator Function)
- **MUST be a generator function** (using `function*` syntax)
- Yields records one at a time using `yield record`
- Each record should be a plain JavaScript object

#### `close()`
- Called when reading is complete (success or error)
- Clean up resources, close streams/connections
- Reset internal state

## StreamHelper API (For File-Based Readers)

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

## Reader Example: API-Based with Pagination

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("API Reader Plugin");

function getAuthFromAdminConfig(authConfig) {
    if (!authConfig) return { type: 'none', token: '', username: '', password: '' };
    var properties = getConfigValue(authConfig, 'properties', null);
    var subType = getConfigValue(authConfig, 'subType', '');
    if (!properties) return { type: 'none', token: '', username: '', password: '' };
    if (subType === 'BEARER_TOKEN') {
        return { type: 'bearer', token: getConfigValue(properties, 'bearerToken', ''), username: '', password: '' };
    } else if (subType === 'BASIC_AUTH') {
        return { type: 'basic', token: '', username: getConfigValue(properties, 'basicAuthUsername', ''), password: getConfigValue(properties, 'basicAuthPassword', '') };
    }
    return { type: 'none', token: '', username: '', password: '' };
}

function apiReaderPlugin(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', '');
    var endpoint = getConfigValue(config, 'endpoint', '/api/items');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);
    var allRecords = [];
    var currentIndex = 0;

    return {
        open: function() {
            var headers = { "Content-Type": "application/json", "Accept": "application/json" };
            if (auth.type === 'bearer' && auth.token) {
                headers["Authorization"] = "Bearer " + auth.token;
            } else if (auth.type === 'basic' && auth.username && auth.password) {
                headers["Authorization"] = "Basic " + base64Encode(auth.username + ':' + auth.password);
            }

            var page = 1;
            var hasMore = true;
            while (hasMore) {
                var url = baseUrl + endpoint + "?page=" + page;
                var data = getJson(url, headers);
                if (data && data.data && data.data.length > 0) {
                    for (var i = 0; i < data.data.length; i++) {
                        allRecords.push(data.data[i]);
                    }
                    journal.onProgress(allRecords.length);
                    page++;
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
    simpleDescription: { en: "API Reader", de: "API Reader" },
    args: [
        { key: "baseUrl", label_en: "API Base URL", label_de: "API Basis-URL", type: "text", required: true, desc_en: "Base URL of the API", desc_de: "Basis-URL der API" },
        { key: "endpoint", label_en: "Endpoint", label_de: "Endpunkt", type: "text", default: "/api/items", desc_en: "API endpoint path", desc_de: "API-Endpunktpfad" },
        { key: "authConfig", label_en: "Authentication", label_de: "Authentifizierung", type: "adminconfig", subType: "BEARER_TOKEN", required: true, desc_en: "Select authentication", desc_de: "Authentifizierung auswählen" }
    ],
    tags: ["dynamic-plugin", "reader"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

## Reader-Specific Best Practices

1. **Always use generator functions** for `readRecords` - Chioro expects `function*` with `yield`
2. **Include the "reader" tag** for discoverability

## Reader-Specific Troubleshooting

| Issue | Solution |
|-------|----------|
| Records not appearing | Ensure `readRecords` is a generator (`function*`) and uses `yield` |
| Stream errors | Always call `streamHelper.open()` before reading |
| Progress not showing | Call `journal.onProgress(count)` during `open()` |

---

# Part 3: Writer Plugins

Writer plugins export data from Chioro to external destinations (APIs, files, etc.).

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
| `config` | Object | Configuration values from the UI |
| `streamHelper` | Object | Helper for writing to file output streams (for file-based writers) |
| `journal` | Object | Progress reporting via `journal.onProgress(count)` |

### Writer Interface Methods

#### `open()`
- Called once when the writer starts
- Initialize connections, authenticate to APIs

#### `writeRecord(record)`
- Called once per record (NOT a generator)
- `record` is a plain JavaScript object with the record data
- Send/write the record to destination

#### `close()`
- Called when writing is complete
- Flush buffers, close connections

## StreamHelper API (For File-Based Writers)

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

## Writer Example: API Writer

```javascript
const Toolpackage = require('chioro-toolbox/toolpackage');
const tools = new Toolpackage("API Writer Plugin");

function getAuthFromAdminConfig(authConfig) {
    if (!authConfig) return { type: 'none', token: '', username: '', password: '' };
    var properties = getConfigValue(authConfig, 'properties', null);
    var subType = getConfigValue(authConfig, 'subType', '');
    if (!properties) return { type: 'none', token: '', username: '', password: '' };
    if (subType === 'BEARER_TOKEN') {
        return { type: 'bearer', token: getConfigValue(properties, 'bearerToken', ''), username: '', password: '' };
    } else if (subType === 'BASIC_AUTH') {
        return { type: 'basic', token: '', username: getConfigValue(properties, 'basicAuthUsername', ''), password: getConfigValue(properties, 'basicAuthPassword', '') };
    }
    return { type: 'none', token: '', username: '', password: '' };
}

function apiWriterPlugin(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', '');
    var endpoint = getConfigValue(config, 'endpoint', '/api/documents');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var headers = {};
    var recordCount = 0;

    return {
        open: function() {
            headers = { "Content-Type": "application/json", "Accept": "application/json" };
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
    simpleDescription: { en: "API Writer", de: "API Writer" },
    args: [
        { key: "baseUrl", label_en: "API Base URL", label_de: "API Basis-URL", type: "text", required: true, desc_en: "Base URL of the API", desc_de: "Basis-URL der API" },
        { key: "endpoint", label_en: "Endpoint", label_de: "Endpunkt", type: "text", default: "/api/documents", desc_en: "API endpoint path", desc_de: "API-Endpunktpfad" },
        { key: "authConfig", label_en: "Authentication", label_de: "Authentifizierung", type: "adminconfig", subType: "BASIC_AUTH", required: true, desc_en: "Select authentication", desc_de: "Authentifizierung auswählen" }
    ],
    tags: ["dynamic-plugin", "writer"],
    hideInToolbox: true
});

tools.exportAll(exports);
```

## Writer-Specific Best Practices

1. **Include the "writer" tag** for discoverability
2. **Flush buffers in close()** for file-based writers

## Key Differences: Readers vs Writers

| Aspect | Reader | Writer |
|--------|--------|--------|
| Record method | `readRecords: function*()` (generator, yields) | `writeRecord: function(record)` (regular function) |
| Data flow | Source → Chioro | Chioro → Destination |
| Tags | `["dynamic-plugin", "reader"]` | `["dynamic-plugin", "writer"]` |
