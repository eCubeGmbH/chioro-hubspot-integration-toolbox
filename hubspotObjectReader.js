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
// Known HubSpot properties requested per entity type, so that the objects
// returned by the CRM Objects API already contain the most relevant fields
// instead of just the small default set HubSpot returns when no
// "properties" query parameter is supplied.
// ---------------------------------------------------------------------------
var HUBSPOT_PROPERTIES = {
    companies: [
        'name', 'domain', 'phone', 'city', 'state', 'country', 'zip',
        'address', 'address2', 'industry', 'numberofemployees', 'annualrevenue',
        'description', 'website', 'lifecyclestage', 'type', 'about_us',
        'founded_year', 'linkedinhandle', 'twitterhandle', 'facebookpage',
        'timezone', 'total_money_raised', 'hs_lead_status', 'hubspot_owner_id',
        'closedate', 'revenue_range', 'industry_group', 'state_code',
        'country_code', 'external_account_id', 'createdate', 'hs_lastmodifieddate'
    ],
    contacts: [
        'email', 'firstname', 'lastname', 'phone', 'mobilephone', 'fax',
        'jobtitle', 'company', 'city', 'state', 'country', 'zip', 'address',
        'website', 'industry', 'annualrevenue', 'lifecyclestage',
        'hs_lead_status', 'hubspot_owner_id', 'hs_email_domain', 'salutation',
        'date_of_birth', 'message', 'numemployees', 'hs_persona',
        'external_contact_id', 'createdate', 'lastmodifieddate'
    ],
    deals: [
        'dealname', 'amount', 'dealstage', 'pipeline', 'closedate',
        'dealtype', 'description', 'hubspot_owner_id', 'hs_priority',
        'hs_forecast_amount', 'hs_forecast_probability',
        'hs_deal_stage_probability', 'hs_next_step', 'external_deal_id',
        'area_of_interest', 'createdate', 'hs_lastmodifieddate'
    ]
};

var PAGE_LIMIT = 100;

/**
 * HubSpot Object Reader.
 *
 * Reads CRM objects (companies, contacts, deals) via the HubSpot CRM
 * Objects API:
 *   GET {baseUrl}/crm/v3/objects/{entity}
 *
 * Pagination follows HubSpot's cursor-based scheme: each page returns
 * paging.next.after, which is passed back as the "after" query parameter
 * to fetch the next page. Iteration stops once no more "next" cursor is
 * returned.
 *
 * Each yielded record is a flattened representation of the HubSpot object:
 * the object id, createdAt/updatedAt/archived metadata, and all requested
 * properties as top-level fields.
 */
function hubspotObjectReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubapi.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);

    var buffer = [];
    var bufferIndex = 0;
    var afterCursor = null;
    var hasMore = true;
    var recordCount = 0;
    var headers = null;

    function buildHeaders() {
        var hdrs = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        };

        if (auth.type === 'bearer' && auth.token) {
            hdrs["Authorization"] = "Bearer " + auth.token;
        } else if (auth.type === 'basic' && auth.username) {
            hdrs["Authorization"] = "Basic " + base64Encode(auth.username + ":" + auth.password);
        }

        return hdrs;
    }

    function normalizeBaseUrl(url) {
        if (!url) return '';
        return url.charAt(url.length - 1) === '/' ? url.slice(0, -1) : url;
    }

    function buildUrl() {
        var propertyNames = HUBSPOT_PROPERTIES[entity] || [];

        var query = [];
        query.push("limit=" + encodeURIComponent(String(PAGE_LIMIT)));
        if (propertyNames.length > 0) {
            query.push("properties=" + encodeURIComponent(propertyNames.join(",")));
        }
        if (afterCursor) {
            query.push("after=" + encodeURIComponent(String(afterCursor)));
        }

        return normalizeBaseUrl(baseUrl) + "/crm/v3/objects/" + encodeURIComponent(entity) + "?" + query.join("&");
    }

    function flattenRecord(item) {
        var flat = {};
        flat.id = item.id;
        flat.createdAt = item.createdAt;
        flat.updatedAt = item.updatedAt;
        flat.archived = item.archived;

        var props = item.properties || {};
        for (var key in props) {
            if (Object.prototype.hasOwnProperty.call(props, key)) {
                flat[key] = props[key];
            }
        }

        return flat;
    }

    function fetchNextPage() {
        if (!hasMore) return;

        var url = buildUrl();
        var data = getJson(url, headers);

        var results = (data && Array.isArray(data.results)) ? data.results : [];
        buffer = [];
        for (var i = 0; i < results.length; i++) {
            buffer.push(flattenRecord(results[i]));
        }
        bufferIndex = 0;

        if (data && data.paging && data.paging.next && data.paging.next.after) {
            afterCursor = data.paging.next.after;
            hasMore = true;
        } else {
            afterCursor = null;
            hasMore = false;
        }
    }

    return {
        open: function() {
            buffer = [];
            bufferIndex = 0;
            afterCursor = null;
            hasMore = true;
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
                    if (buffer.length === 0) {
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
            buffer = [];
            bufferIndex = 0;
            afterCursor = null;
            hasMore = true;
            recordCount = 0;
            headers = null;
        }
    };
}

module.exports = hubspotObjectReader;
