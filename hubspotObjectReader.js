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

// Properties always included in the writable property list for a given
// entity, regardless of what the CRM Properties API's modificationMetadata
// reports. "hs_legal_basis" is HubSpot's GDPR "Legal basis for processing
// contact's data" property; it is kept here for testing purposes.
var FORCED_WRITABLE_PROPERTIES = {
    contacts: ['hs_legal_basis']
};

var PAGE_LIMIT = 100;
var ASSOCIATION_PAGE_LIMIT = 500;

// HubSpot batch endpoints (both the v4 associations batch/read and the v3
// objects batch/read endpoints) are called in chunks of at most this many
// ids per request.
var BATCH_LIMIT = 100;

/**
 * HubSpot Object Reader.
 *
 * Reads CRM objects (companies, contacts, deals) via the HubSpot CRM
 * Objects API:
 *   GET {baseUrl}/crm/v3/objects/{entity}
 *
 * Before reading objects, the reader first calls the CRM Properties API
 * (GET {baseUrl}/crm/v3/properties/{entity}) to discover the full set of
 * property definitions for the entity, both HubSpot-defined and custom.
 * From those definitions it resolves every *writable* property, i.e.
 * every property whose "modificationMetadata.readOnlyValue" flag is not
 * true (see resolveWritablePropertyNames/isPropertyWritable below) — this
 * automatically includes custom fields as well as any standard field
 * HubSpot allows the API to write to, without requiring manual
 * configuration. Should the Properties API call fail for any reason, the
 * reader falls back to the curated HUBSPOT_PROPERTIES list merged with
 * any custom (non HubSpot-defined) properties, the same way it always
 * did. A small FORCED_WRITABLE_PROPERTIES list adds specific properties
 * regardless of what HubSpot's modificationMetadata reports — currently
 * just "hs_legal_basis" on contacts, kept for testing purposes.
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
 * When entity === 'object associations', the reader fetches all four
 * fixed association pairs in one go, one after another:
 *   deals -> companies
 *   deals -> contacts
 *   contacts -> companies
 *   companies -> companies
 * For every pair, every object of the "from" type is enumerated, and for
 * each one its associations to the "to" type are fetched. Each yielded
 * record represents a single association (one fromObjectId/toObjectId
 * pair plus its association type metadata).
 *
 * As a fifth "entity" option ("users"), the reader retrieves the HubSpot
 * account's users via HubSpot's Settings User Provisioning API:
 *   GET {baseUrl}/settings/users/v3
 * Pagination follows the same cursor-based scheme (paging.next.after /
 * "after" query parameter) as the CRM Objects API. Each yielded record is
 * a flattened representation of the HubSpot user: id, email, firstName,
 * lastName, primaryTeamId, roleId, roleIds (semicolon-separated),
 * secondaryTeamIds (semicolon-separated), sendWelcomeEmail and superAdmin.
 *
 * As a seventh "entity" option ("deal stages"), the reader retrieves the
 * deal pipelines together with their stages via HubSpot's CRM Pipelines
 * API:
 *   GET {baseUrl}/crm/v3/pipelines/deals
 * This endpoint is not paginated; it returns every deal pipeline in one
 * response, each with its nested "stages" array. Each yielded record
 * represents a single pipeline stage, flattened together with the
 * metadata of the pipeline it belongs to (pipeline id/label/displayOrder/
 * archived/createdAt/updatedAt) plus the stage's own fields (id/label/
 * displayOrder/archived/createdAt/updatedAt/writePermissions) and all
 * entries of the stage's "metadata" map (e.g. "probability" and
 * "isClosed") spread as top-level fields.
 *
 * As a sixth "entity" option ("activities"), the reader retrieves the
 * activities (calls, emails, notes, meetings, tasks) associated with
 * companies, contacts and deals, for all three entities in a single run.
 * Unlike "object associations", this mode uses HubSpot's batch endpoints
 * instead of looping one object at a time:
 *   GET  {baseUrl}/crm/v3/objects/{sourceEntity}                                  (enumerate all source object ids, paginated)
 *   POST {baseUrl}/crm/v4/associations/{sourceEntity}/{activityType}/batch/read   (batch-read associations for up to BATCH_LIMIT ids at once)
 *   POST {baseUrl}/crm/v3/objects/{activityType}/batch/read                       (batch-read the associated activity objects for up to BATCH_LIMIT ids at once)
 * For every one of the 3 source entities x 5 activity types combinations,
 * every source object id is batched into groups of BATCH_LIMIT, the
 * associations for each group are fetched in a single batch call, the
 * distinct activity ids referenced by those associations are collected,
 * and the full activity objects are then fetched via batched object reads
 * (instead of one request per activity). Each yielded record combines the
 * association metadata (source entity/id, activity type/id, association
 * category/type/label) with the flattened activity object properties.
 */

// Fixed set of association pairs fetched when entity === 'object associations'.
var ASSOCIATION_PAIRS = [
    { fromEntity: 'deals', toEntity: 'companies' },
    { fromEntity: 'deals', toEntity: 'contacts' },
    { fromEntity: 'contacts', toEntity: 'companies' },
    { fromEntity: 'companies', toEntity: 'companies' }
];

// Fixed set of source entities and activity types fetched when
// entity === 'activities'. All 3 x 5 = 15 combinations are read in one run.
var ACTIVITY_SOURCE_ENTITIES = ['companies', 'contacts', 'deals'];
var ACTIVITY_TYPES = ['calls', 'emails', 'notes', 'meetings', 'tasks'];

// Curated standard properties requested per activity type, merged at
// runtime with any custom (non HubSpot-defined) properties discovered via
// the CRM Properties API, the same way HUBSPOT_PROPERTIES is handled for
// companies/contacts/deals.
var ACTIVITY_PROPERTIES = {
    calls: [
        'hs_call_title', 'hs_call_body', 'hs_call_duration', 'hs_call_direction',
        'hs_call_disposition', 'hs_call_status', 'hs_call_source',
        'hs_call_from_number', 'hs_call_to_number', 'hs_call_recording_url',
        'hs_call_callee_object_id', 'hs_call_callee_object_type',
        'hs_activity_type', 'hs_timestamp', 'hubspot_owner_id',
        'hs_created_by', 'hs_createdate', 'hs_lastmodifieddate'
    ],
    emails: [
        'hs_email_subject', 'hs_email_text', 'hs_email_html', 'hs_email_status',
        'hs_email_direction', 'hs_email_from_email', 'hs_email_from_firstname',
        'hs_email_from_lastname', 'hs_email_to_email', 'hs_email_to_firstname',
        'hs_email_to_lastname', 'hs_email_headers', 'hs_activity_type',
        'hs_timestamp', 'hubspot_owner_id', 'hs_created_by', 'hs_createdate',
        'hs_lastmodifieddate'
    ],
    notes: [
        'hs_note_body', 'hs_attachment_ids', 'hs_activity_type', 'hs_timestamp',
        'hubspot_owner_id', 'hs_created_by', 'hs_createdate', 'hs_lastmodifieddate'
    ],
    meetings: [
        'hs_meeting_title', 'hs_meeting_body', 'hs_meeting_location',
        'hs_meeting_start_time', 'hs_meeting_end_time', 'hs_meeting_outcome',
        'hs_meeting_external_url', 'hs_internal_meeting_notes',
        'hs_activity_type', 'hs_timestamp', 'hubspot_owner_id', 'hs_created_by',
        'hs_createdate', 'hs_lastmodifieddate'
    ],
    tasks: [
        'hs_task_subject', 'hs_task_body', 'hs_task_status', 'hs_task_priority',
        'hs_task_type', 'hs_task_completion_date', 'hs_task_is_all_day',
        'hs_task_reminders', 'hs_activity_type', 'hs_timestamp',
        'hubspot_owner_id', 'hs_created_by', 'hs_createdate', 'hs_lastmodifieddate'
    ]
};

function hubspotObjectReader(config, streamHelper, journal) {
    var baseUrl = getConfigValue(config, 'baseUrl', 'https://api.hubapi.com');
    var entity = getConfigValue(config, 'entity', 'companies');
    var authConfig = getConfigValue(config, 'authConfig', null);
    var auth = getAuthFromAdminConfig(authConfig);
    var isAssociations = entity === 'object associations';
    var isActivities = entity === 'activities';
    var isUsers = entity === 'users';
    var isDealStages = entity === 'deal stages';

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

    // State used only when isActivities === true. All records are built
    // up-front in open() (via buildActivityRecords) and then simply
    // streamed out of this buffer by readRecords().
    var activityBuffer = [];
    var activityBufferIndex = 0;

    // State used only when isDealStages === true. All records are built
    // up-front in open() (via buildDealStageRecords) and then simply
    // streamed out of this buffer by readRecords().
    var dealStageBuffer = [];
    var dealStageBufferIndex = 0;

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
     * Converts a value to its text representation, leaving null/undefined
     * untouched. Used to ensure numeric-looking HubSpot fields (object ids,
     * association type ids, etc.) are written out as text rather than
     * numbers.
     */
    function toText(value) {
        if (value === null || value === undefined) {
            return value;
        }
        return String(value);
    }

    /**
     * Calls the CRM Properties API (GET /crm/v3/properties/{objectType})
     * to discover the property definitions for a given object type.
     * Returns an empty array if the call fails for any reason, so callers
     * can fall back to their curated property list alone.
     */
    function fetchPropertyDefinitions(objectType) {
        try {
            var url = normalizeBaseUrl(baseUrl) + '/crm/v3/properties/' + encodeURIComponent(objectType);
            var data = getJson(url, headers);
            return (data && Array.isArray(data.results)) ? data.results : [];
        } catch (e) {
            return [];
        }
    }

    /**
     * Merges a curated list of property names with any custom (non
     * HubSpot-defined) property definitions, preserving curated order and
     * skipping duplicates.
     */
    function mergeCustomProperties(curated, definitions) {
        var merged = curated.slice();
        var seen = {};
        for (var c = 0; c < merged.length; c++) {
            seen[merged[c]] = true;
        }

        for (var i = 0; i < definitions.length; i++) {
            var def = definitions[i];
            var isCustom = def && def.hubspotDefined !== true;
            if (isCustom && def.name && !seen[def.name]) {
                seen[def.name] = true;
                merged.push(def.name);
            }
        }

        return merged;
    }

    /**
     * Returns true if a property definition (as returned by the CRM
     * Properties API) can be written to via the API: its
     * "modificationMetadata.readOnlyValue" flag must not be true, and the
     * property must not be archived. Properties without modificationMetadata
     * are treated as writable, matching HubSpot's own default behavior.
     */
    function isPropertyWritable(def) {
        if (!def || def.archived === true) {
            return false;
        }
        var metadata = def.modificationMetadata || {};
        return metadata.readOnlyValue !== true;
    }

    /**
     * Builds the list of all writable property names for the configured
     * entity (companies, contacts, deals) from the CRM Properties API
     * results: only properties passing isPropertyWritable() are kept,
     * covering both HubSpot-defined and custom properties alike. Any
     * names listed in FORCED_WRITABLE_PROPERTIES for the entity are
     * always appended as well, even if HubSpot's modificationMetadata
     * marks them as read-only (used for testing purposes, e.g.
     * "hs_legal_basis" on contacts).
     */
    function resolveWritablePropertyNames(definitions) {
        var names = [];
        var seen = {};

        for (var i = 0; i < definitions.length; i++) {
            var def = definitions[i];
            if (def && def.name && isPropertyWritable(def) && !seen[def.name]) {
                seen[def.name] = true;
                names.push(def.name);
            }
        }

        var forced = FORCED_WRITABLE_PROPERTIES[entity] || [];
        for (var f = 0; f < forced.length; f++) {
            if (!seen[forced[f]]) {
                seen[forced[f]] = true;
                names.push(forced[f]);
            }
        }

        return names;
    }

    /**
     * Discovers the property names for the configured entity. For
     * companies, contacts and deals this returns *all* writable
     * properties (see resolveWritablePropertyNames), including custom
     * ones. If the CRM Properties API call fails (returns no
     * definitions), falls back to the curated HUBSPOT_PROPERTIES list
     * merged with any custom (non HubSpot-defined) properties, plus the
     * entity's forced properties.
     */
    function resolvePropertyNames() {
        var curated = HUBSPOT_PROPERTIES[entity] || [];
        var definitions = fetchPropertyDefinitions(entity);
        var forced = FORCED_WRITABLE_PROPERTIES[entity] || [];

        if (definitions.length === 0) {
            var merged = mergeCustomProperties(curated, definitions);
            for (var f = 0; f < forced.length; f++) {
                if (merged.indexOf(forced[f]) === -1) {
                    merged.push(forced[f]);
                }
            }
            return merged;
        }

        return resolveWritablePropertyNames(definitions);
    }

    /**
     * Discovers the property names for a given activity type (calls,
     * emails, notes, meetings, tasks), including custom properties, the
     * same way resolvePropertyNames() does for companies/contacts/deals.
     */
    function resolveActivityPropertyNames(activityType) {
        var curated = ACTIVITY_PROPERTIES[activityType] || [];
        return mergeCustomProperties(curated, fetchPropertyDefinitions(activityType));
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

    /**
     * Builds the URL for a page of the Settings User Provisioning API
     * (GET /settings/users/v3), used when entity === 'users'.
     */
    function buildUsersUrl() {
        var query = [];
        query.push("limit=" + encodeURIComponent(String(PAGE_LIMIT)));
        if (afterCursor) {
            query.push("after=" + encodeURIComponent(String(afterCursor)));
        }

        return normalizeBaseUrl(baseUrl) + "/settings/users/v3?" + query.join("&");
    }

    /**
     * Joins an array of values into a semicolon-separated string, the same
     * convention used elsewhere for multi-value HubSpot fields. Returns an
     * empty string for missing/empty arrays.
     */
    function joinMultiValue(arr) {
        return (Array.isArray(arr) && arr.length > 0) ? arr.join(';') : '';
    }

    /**
     * Flattens a HubSpot PublicUser object (as returned by the Settings
     * User Provisioning API) into a plain record.
     */
    function flattenUserRecord(item) {
        item = item || {};
        return {
            id: toText(item.id),
            email: item.email,
            firstName: item.firstName,
            lastName: item.lastName,
            primaryTeamId: toText(item.primaryTeamId),
            roleId: toText(item.roleId),
            roleIds: joinMultiValue(item.roleIds),
            secondaryTeamIds: joinMultiValue(item.secondaryTeamIds),
            sendWelcomeEmail: item.sendWelcomeEmail,
            superAdmin: item.superAdmin
        };
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

    /**
     * Builds the URL for the CRM Pipelines API (GET /crm/v3/pipelines/deals),
     * used when entity === 'deal stages'. This endpoint is not paginated.
     */
    function buildDealPipelinesUrl() {
        return normalizeBaseUrl(baseUrl) + "/crm/v3/pipelines/deals";
    }

    /**
     * Flattens a single HubSpot deal pipeline stage together with the
     * pipeline it belongs to into one plain record. The stage's own
     * "metadata" map (e.g. probability, isClosed) is spread as top-level
     * fields, the same way flattenRecord() spreads an object's properties.
     */
    function flattenDealStageRecord(pipeline, stage) {
        pipeline = pipeline || {};
        stage = stage || {};

        var flat = {
            pipelineId: toText(pipeline.id),
            pipelineLabel: pipeline.label,
            pipelineDisplayOrder: pipeline.displayOrder,
            pipelineArchived: pipeline.archived,
            pipelineArchivedAt: pipeline.archivedAt,
            pipelineCreatedAt: pipeline.createdAt,
            pipelineUpdatedAt: pipeline.updatedAt,
            stageId: toText(stage.id),
            stageLabel: stage.label,
            stageDisplayOrder: stage.displayOrder,
            stageArchived: stage.archived,
            stageArchivedAt: stage.archivedAt,
            stageCreatedAt: stage.createdAt,
            stageUpdatedAt: stage.updatedAt,
            stageWritePermissions: stage.writePermissions
        };

        var metadata = stage.metadata || {};
        for (var key in metadata) {
            if (Object.prototype.hasOwnProperty.call(metadata, key)) {
                flat[key] = metadata[key];
            }
        }

        return flat;
    }

    /**
     * Fetches all deal pipelines (with their nested stages) in a single
     * call and appends one flattened record per pipeline stage to
     * dealStageBuffer. Called once from open() when entity === 'deal
     * stages'.
     */
    function buildDealStageRecords() {
        var data = getJson(buildDealPipelinesUrl(), headers);
        var pipelines = (data && Array.isArray(data.results)) ? data.results : [];

        for (var p = 0; p < pipelines.length; p++) {
            var pipeline = pipelines[p];
            var stages = (pipeline && Array.isArray(pipeline.stages)) ? pipeline.stages : [];

            for (var s = 0; s < stages.length; s++) {
                dealStageBuffer.push(flattenDealStageRecord(pipeline, stages[s]));
                recordCount++;
                if (journal && journal.onProgress) {
                    journal.onProgress(recordCount);
                }
            }
        }
    }

    function fetchNextPage() {
        if (!hasMore) return;

        var url = isUsers ? buildUsersUrl() : buildUrl();
        var data = getJson(url, headers);

        var results = (data && Array.isArray(data.results)) ? data.results : [];
        buffer = [];
        for (var i = 0; i < results.length; i++) {
            buffer.push(isUsers ? flattenUserRecord(results[i]) : flattenRecord(results[i]));
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
                        toObjectId: toText(result.toObjectId),
                        associationCategory: associationType.category,
                        associationTypeId: toText(associationType.typeId),
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

    // -----------------------------------------------------------------------
    // Activities mode helpers (batch association reads + batch object reads)
    // -----------------------------------------------------------------------

    /**
     * Splits an array into chunks of at most `size` items.
     */
    function chunkArray(arr, size) {
        var chunks = [];
        for (var i = 0; i < arr.length; i += size) {
            chunks.push(arr.slice(i, i + size));
        }
        return chunks;
    }

    /**
     * Enumerates and returns *all* object ids for the given entity type by
     * fully paginating the plain CRM Objects API (no properties needed).
     */
    function fetchAllObjectIds(entityType) {
        var ids = [];
        var after = null;
        var more = true;

        while (more) {
            var query = [];
            query.push("limit=" + encodeURIComponent(String(PAGE_LIMIT)));
            if (after) {
                query.push("after=" + encodeURIComponent(String(after)));
            }

            var url = normalizeBaseUrl(baseUrl) + "/crm/v3/objects/" + encodeURIComponent(entityType) + "?" + query.join("&");
            var data = getJson(url, headers);
            var results = (data && Array.isArray(data.results)) ? data.results : [];
            for (var i = 0; i < results.length; i++) {
                ids.push(results[i].id);
            }

            if (data && data.paging && data.paging.next && data.paging.next.after) {
                after = data.paging.next.after;
                more = true;
            } else {
                more = false;
            }
        }

        return ids;
    }

    /**
     * Appends one association record (with its association type metadata)
     * per fromObjectId/toObjectId/associationType combination found in a
     * batch/read "to" list.
     */
    function appendAssociationRecords(records, fromId, toList) {
        var list = Array.isArray(toList) ? toList : [];
        for (var i = 0; i < list.length; i++) {
            var to = list[i];
            var types = (to && Array.isArray(to.associationTypes) && to.associationTypes.length > 0)
                ? to.associationTypes
                : [{}];

            for (var t = 0; t < types.length; t++) {
                var associationType = types[t] || {};
                records.push({
                    fromObjectId: toText(fromId),
                    toObjectId: toText(to.toObjectId),
                    associationCategory: associationType.category,
                    associationTypeId: toText(associationType.typeId),
                    associationLabel: associationType.label
                });
            }
        }
    }

    /**
     * Batch-reads the associations from up to BATCH_LIMIT "from" ids to the
     * given "to" entity type in a single POST call via the v4 Associations
     * batch/read API, instead of one request per object. If a given "from"
     * id has more associated objects than fit on one page, the paging
     * cursor returned for that specific id is followed with additional
     * single-id continuation calls until exhausted.
     */
    function fetchAssociationBatch(fromEntity, toEntity, ids) {
        var records = [];
        if (!ids || ids.length === 0) {
            return records;
        }

        var url = normalizeBaseUrl(baseUrl) + '/crm/v4/associations/' +
            encodeURIComponent(fromEntity) + '/' + encodeURIComponent(toEntity) + '/batch/read';

        var body = {
            inputs: ids.map(function(id) {
                return { id: id };
            })
        };
        var data = postJson(url, body, headers);
        var results = (data && Array.isArray(data.results)) ? data.results : [];

        for (var i = 0; i < results.length; i++) {
            var result = results[i];
            var fromId = (result && result.from) ? result.from.id : null;
            appendAssociationRecords(records, fromId, result && result.to);

            var cursor = (result && result.paging && result.paging.next && result.paging.next.after)
                ? result.paging.next.after
                : null;

            while (cursor) {
                var contBody = { inputs: [{ id: fromId, after: cursor }] };
                var contData = postJson(url, contBody, headers);
                var contResults = (contData && Array.isArray(contData.results)) ? contData.results : [];
                var contResult = contResults.length > 0 ? contResults[0] : null;

                appendAssociationRecords(records, fromId, contResult && contResult.to);

                cursor = (contResult && contResult.paging && contResult.paging.next && contResult.paging.next.after)
                    ? contResult.paging.next.after
                    : null;
            }
        }

        return records;
    }

    /**
     * Batch-reads up to BATCH_LIMIT activity objects (of the given
     * activity type) in a single POST call via the v3 Objects batch/read
     * API, instead of one GET request per activity. Returns a map of
     * activity id -> flattened activity record.
     */
    function fetchActivityObjectsBatch(activityType, ids, properties) {
        var map = {};
        if (!ids || ids.length === 0) {
            return map;
        }

        var url = normalizeBaseUrl(baseUrl) + '/crm/v3/objects/' + encodeURIComponent(activityType) + '/batch/read';
        var body = {
            properties: properties,
            inputs: ids.map(function(id) {
                return { id: id };
            })
        };
        var data = postJson(url, body, headers);
        var results = (data && Array.isArray(data.results)) ? data.results : [];

        for (var i = 0; i < results.length; i++) {
            var item = results[i];
            map[toText(item.id)] = flattenRecord(item);
        }

        return map;
    }

    /**
     * Builds the full set of activity records (one per source
     * object/activity association) for all ACTIVITY_SOURCE_ENTITIES x
     * ACTIVITY_TYPES combinations, using batch association reads followed
     * by batch object reads, and appends them to activityBuffer. Called
     * once from open() when entity === 'activities'.
     */
    function buildActivityRecords() {
        var activityPropertiesByType = {};
        for (var a = 0; a < ACTIVITY_TYPES.length; a++) {
            activityPropertiesByType[ACTIVITY_TYPES[a]] = resolveActivityPropertyNames(ACTIVITY_TYPES[a]);
        }

        for (var s = 0; s < ACTIVITY_SOURCE_ENTITIES.length; s++) {
            var sourceEntity = ACTIVITY_SOURCE_ENTITIES[s];
            var sourceIds = fetchAllObjectIds(sourceEntity);
            var sourceIdBatches = chunkArray(sourceIds, BATCH_LIMIT);

            for (var t = 0; t < ACTIVITY_TYPES.length; t++) {
                var activityType = ACTIVITY_TYPES[t];

                // 1) Batch-read associations for every chunk of source ids.
                var associationRecords = [];
                for (var b = 0; b < sourceIdBatches.length; b++) {
                    associationRecords = associationRecords.concat(
                        fetchAssociationBatch(sourceEntity, activityType, sourceIdBatches[b])
                    );
                }

                if (associationRecords.length === 0) {
                    continue;
                }

                // 2) Collect the distinct activity ids referenced by those
                // associations, then batch-read the activity objects.
                var activityIds = [];
                var seenActivityIds = {};
                for (var r = 0; r < associationRecords.length; r++) {
                    var activityId = associationRecords[r].toObjectId;
                    if (!seenActivityIds[activityId]) {
                        seenActivityIds[activityId] = true;
                        activityIds.push(activityId);
                    }
                }

                var activityMap = {};
                var activityIdBatches = chunkArray(activityIds, BATCH_LIMIT);
                for (var ib = 0; ib < activityIdBatches.length; ib++) {
                    var fetched = fetchActivityObjectsBatch(activityType, activityIdBatches[ib], activityPropertiesByType[activityType]);
                    for (var fetchedId in fetched) {
                        if (Object.prototype.hasOwnProperty.call(fetched, fetchedId)) {
                            activityMap[fetchedId] = fetched[fetchedId];
                        }
                    }
                }

                // 3) Combine each association with its activity's
                // properties, keeping the association info in the result.
                for (var ar = 0; ar < associationRecords.length; ar++) {
                    var assoc = associationRecords[ar];
                    var activity = activityMap[assoc.toObjectId] || null;

                    var combined = {
                        fromEntity: sourceEntity,
                        fromObjectId: assoc.fromObjectId,
                        activityType: activityType,
                        activityId: assoc.toObjectId,
                        associationCategory: assoc.associationCategory,
                        associationTypeId: assoc.associationTypeId,
                        associationLabel: assoc.associationLabel
                    };

                    if (activity) {
                        for (var key in activity) {
                            if (Object.prototype.hasOwnProperty.call(activity, key) && key !== 'id') {
                                combined[key] = activity[key];
                            }
                        }
                    }

                    activityBuffer.push(combined);
                    recordCount++;
                    if (journal && journal.onProgress) {
                        journal.onProgress(recordCount);
                    }
                }
            }
        }
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

            if (isActivities) {
                activityBuffer = [];
                activityBufferIndex = 0;
                recordCount = 0;
                buildActivityRecords();
                return;
            }

            if (isDealStages) {
                dealStageBuffer = [];
                dealStageBufferIndex = 0;
                recordCount = 0;
                buildDealStageRecords();
                return;
            }

            buffer = [];
            bufferIndex = 0;
            afterCursor = null;
            hasMore = true;
            recordCount = 0;

            if (isUsers) {
                propertyNames = [];
                return;
            }

            propertyNames = resolvePropertyNames();
        },

        readRecords: function*() {
            if (isActivities) {
                while (activityBufferIndex < activityBuffer.length) {
                    yield activityBuffer[activityBufferIndex++];
                }
                return;
            }

            if (isDealStages) {
                while (dealStageBufferIndex < dealStageBuffer.length) {
                    yield dealStageBuffer[dealStageBufferIndex++];
                }
                return;
            }

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

            activityBuffer = [];
            activityBufferIndex = 0;

            dealStageBuffer = [];
            dealStageBufferIndex = 0;
        }
    };
}

module.exports = hubspotObjectReader;
