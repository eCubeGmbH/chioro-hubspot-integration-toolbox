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
 * HubSpot Schema Reader.
 *
 * Reads the property schema (field definitions) of a HubSpot CRM object type
 * (companies, contacts, deals) via the HubSpot CRM Properties API:
 *   GET {baseUrl}/crm/v3/properties/{entity}
 *
 * Each yielded record represents one property/field definition of the
 * selected entity (name, label, type, fieldType, description, options, ...).
 */
function hubspotSchemaReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubapi.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);

    var properties = [];
    var index = 0;

    function buildHeaders() {
        var hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        };

        if (auth.type === 'bearer' && auth.token) {
            hdrs["Authorization"] = "Bearer " + auth.token;
        }

        return hdrs;
    }

    function normalizeBaseUrl(url) {
        if (!url) return '';
        return url.charAt(url.length - 1) === '/' ? url.slice(0, -1) : url;
    }

    return {
        open: function() {
            properties = [];
            index = 0;

            var headers = buildHeaders();
            var url = normalizeBaseUrl(baseUrl) + '/crm/v3/properties/' + encodeURIComponent(entity);
            var data = getJson(url, headers);

            if (data && Array.isArray(data.results)) {
                properties = data.results;
            }

            if (journal && journal.onProgress) {
                journal.onProgress(properties.length);
            }
        },

        readRecords: function*() {
            while (index < properties.length) {
                yield properties[index++];
            }
        },

        close: function() {
            properties = [];
            index = 0;
        }
    };
}

module.exports = hubspotSchemaReader;
