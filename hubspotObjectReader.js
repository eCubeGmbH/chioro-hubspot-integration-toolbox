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
var ASSOCIATION_PAGE_LIMIT = 500;

/**
 * HubSpot Object Reader.
 *
 * Reads CRM objects (companies, contacts, deals) via the HubSpot CRM
 * Objects API:
 *   GET {baseUrl}/crm/v3/objects/{entity}
 *
 * Before reading objects, the reader first calls the CRM Properties API
 * (GET {baseUrl}/crm/v3/properties/{entity}) to discover the full set of
 * property names defined for the entity, including any custom (non
 * HubSpot-defined) properties. Those custom property names are merged
 * with the curated HUBSPOT_PROPERTIES list so that custom fields are
 * automatically included in every object request without requiring
 * manual configuration.
 *
 * Pagination follows HubSpot's cursor-based scheme: each page returns
 * paging.next.after, which is passed back as the "after" query parameter
 * to fetch the next page. Iteration stops once no more "next" cursor is
 * returned.
 *
 * Each yielded record is a flattened representation of the HubSpot object:
 * the object id, createdAt/updatedAt/archived metadata, and all requested
 * properties as top-level fields.
 *
 * As a fourth "entity" option ("object associations"), the reader also
 * supports reading the associations between HubSpot CRM objects via
 * HubSpot's v4 Associations API:
 *   GET {baseUrl}/crm/v3/objects/{fromEntity}                       (list all "from" object ids, paginated)
 *   GET {baseUrl}/crm/v4/objects/{fromEntity}/{id}/associations/{toEntity}  (list associations per object, paginated)
 *
 * When entity === 'object associations', the reader fetches all three
 * fixed association pairs in one go, one after another:
 *   deals -> companies
 *   deals -> contacts
 *   contacts -> companies
 * For every pair, every object of the "from" type is enumerated, and for
 * each one its associations to the "to" type are fetched. Each yielded
 * record represents a single association (one fromObjectId/toObjectId
 * pair plus its association type metadata).
 */

// Fixed set of association pairs fetched when entity === 'object associations'.
var ASSOCIATION_PAIRS = [
    { fromEntity: 'deals', toEntity: 'companies' },
    { fromEntity: 'deals', toEntity: 'contacts' },
    { fromEntity: 'contacts', toEntity: 'companies' }
];

function hubspotObjectReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubapi.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);
    var isAssociations = entity === 'object associations';

    var buffer = [];
    var bufferIndex = 0;
    var afterCursor = null;
    var hasMore = true;
    var recordCount = 0;
    var headers = null;
    var propertyNames = [];

    // State used only when isAssociations === true.
    var pairIndex = 0;
    var sourceBuffer = [];
    var sourceBufferIndex = 0;
    var sourceAfterCursor = null;
    var sourceHasMore = true;
    var associationBuffer = [];
    var associationBufferIndex = 0;

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

    /**
     * Calls the CRM Properties API to discover the property names defined
     * for the entity, including custom (non HubSpot-defined) properties.
     * Returns the curated HUBSPOT_PROPERTIES list merged with any custom
     * property names found. Falls back to the curated list alone if the
     * properties endpoint call fails for any reason.
     */
    function resolvePropertyNames() {
        var curated = HUBSPOT_PROPERTIES[entity] || [];
        var merged = curated.slice();
        var seen = {};
        for (var c = 0; c < merged.length; c++) {
            seen[merged[c]] = true;
        }

        try {
            var url = normalizeBaseUrl(baseUrl) + '/crm/v3/properties/' + encodeURIComponent(entity);
            var data = getJson(url, headers);
            var definitions = (data && Array.isArray(data.results)) ? data.results : [];

            for (var i = 0; i < definitions.length; i++) {
                var def = definitions[i];
                var isCustom = def && def.hubspotDefined !== true;
                if (isCustom && def.name && !seen[def.name]) {
                    seen[def.name] = true;
                    merged.push(def.name);
                }
            }
        } catch (e) {
            // If the properties endpoint call fails, fall back to the
            // curated property list so object reading can still proceed.
        }

        return merged;
    }

    function buildUrl() {
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

    // -----------------------------------------------------------------------
    // Associations mode helpers
    // -----------------------------------------------------------------------

    /**
     * Fetches the next page of object ids for the given "from" entity type,
     * using the plain CRM Objects API (no properties needed, only ids).
     */
    function fetchNextSourcePage(fromEntity) {
        if (!sourceHasMore) return;

        var query = [];
        query.push("limit=" + encodeURIComponent(String(PAGE_LIMIT)));
        if (sourceAfterCursor) {
            query.push("after=" + encodeURIComponent(String(sourceAfterCursor)));
        }

        var url = normalizeBaseUrl(baseUrl) + "/crm/v3/objects/" + encodeURIComponent(fromEntity) + "?" + query.join("&");
        var data = getJson(url, headers);

        var results = (data && Array.isArray(data.results)) ? data.results : [];
        sourceBuffer = [];
        for (var i = 0; i < results.length; i++) {
            sourceBuffer.push(results[i].id);
        }
        sourceBufferIndex = 0;

        if (data && data.paging && data.paging.next && data.paging.next.after) {
            sourceAfterCursor = data.paging.next.after;
            sourceHasMore = true;
        } else {
            sourceAfterCursor = null;
            sourceHasMore = false;
        }
    }

    /**
     * Fetches (and fully paginates through) all associations from a single
     * "from" object to the "to" entity type, via the v4 Associations API.
     * Returns a flat array of association records, one per
     * fromObjectId/toObjectId/associationType combination.
     */
    function fetchAssociationsForObject(fromId, fromEntity, toEntity) {
        var records = [];
        var after = null;
        var more = true;

        while (more) {
            var query = [];
            query.push("limit=" + encodeURIComponent(String(ASSOCIATION_PAGE_LIMIT)));
            if (after) {
                query.push("after=" + encodeURIComponent(String(after)));
            }

            var url = normalizeBaseUrl(baseUrl) + "/crm/v4/objects/" + encodeURIComponent(fromEntity) +
                "/" + encodeURIComponent(fromId) + "/associations/" + encodeURIComponent(toEntity) +
                "?" + query.join("&");
            var data = getJson(url, headers);

            var results = (data && Array.isArray(data.results)) ? data.results : [];
            for (var i = 0; i < results.length; i++) {
                var result = results[i];
                var types = (result && Array.isArray(result.associationTypes) && result.associationTypes.length > 0)
                    ? result.associationTypes
                    : [{}];

                for (var t = 0; t < types.length; t++) {
                    var associationType = types[t] || {};
                    records.push({
                        fromEntity: fromEntity,
                        fromObjectId: fromId,
                        toEntity: toEntity,
                        toObjectId: result.toObjectId,
                        associationCategory: associationType.category,
                        associationTypeId: associationType.typeId,
                        associationLabel: associationType.label
                    });
                }
            }

            if (data && data.paging && data.paging.next && data.paging.next.after) {
                after = data.paging.next.after;
                more = true;
            } else {
                after = null;
                more = false;
            }
        }

        return records;
    }

    return {
        open: function() {
            headers = buildHeaders();

            if (isAssociations) {
                pairIndex = 0;
                sourceBuffer = [];
                sourceBufferIndex = 0;
                sourceAfterCursor = null;
                sourceHasMore = true;
                associationBuffer = [];
                associationBufferIndex = 0;
                recordCount = 0;
                return;
            }

            buffer = [];
            bufferIndex = 0;
            afterCursor = null;
            hasMore = true;
            recordCount = 0;
            propertyNames = resolvePropertyNames();
        },

        readRecords: function*() {
            if (isAssociations) {
                while (pairIndex < ASSOCIATION_PAIRS.length) {
                    var pair = ASSOCIATION_PAIRS[pairIndex];

                    while (true) {
                        if (associationBufferIndex < associationBuffer.length) {
                            recordCount++;
                            if (journal && journal.onProgress) {
                                journal.onProgress(recordCount);
                            }
                            yield associationBuffer[associationBufferIndex++];
                            continue;
                        }

                        if (sourceBufferIndex >= sourceBuffer.length) {
                            if (!sourceHasMore) {
                                break; // done enumerating this pair's "from" objects
                            }
                            fetchNextSourcePage(pair.fromEntity);
                            if (sourceBuffer.length === 0) {
                                break;
                            }
                        }

                        var fromId = sourceBuffer[sourceBufferIndex++];
                        associationBuffer = fetchAssociationsForObject(fromId, pair.fromEntity, pair.toEntity);
                        associationBufferIndex = 0;
                    }

                    // Move on to the next association pair, resetting the
                    // per-pair pagination state.
                    pairIndex++;
                    sourceBuffer = [];
                    sourceBufferIndex = 0;
                    sourceAfterCursor = null;
                    sourceHasMore = true;
                    associationBuffer = [];
                    associationBufferIndex = 0;
                }
                return;
            }

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
            propertyNames = [];

            pairIndex = 0;
            sourceBuffer = [];
            sourceBufferIndex = 0;
            sourceAfterCursor = null;
            sourceHasMore = true;
            associationBuffer = [];
            associationBufferIndex = 0;
        }
    };
}

module.exports = hubspotObjectReader;
