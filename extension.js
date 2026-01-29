/**
 * Pipe-separated file reader plugin for Chioro.
 *
 * This is a simple reader that parses pipe-separated (|) files.
 * It demonstrates how to create a custom reader plugin.
 */

const Toolpackage = require('chioro-toolbox/toolpackage')

const tools = new Toolpackage("Pipe Reader Tools")
tools.description = 'Plugin for reading pipe-separated files'

/**
 * Pipe reader plugin function.
 *
 * @param {Object} config - Configuration object with reader settings
 * @param {Object} streamHelper - Helper for reading input stream
 * @param {Object} journal - Journal for progress reporting
 * @returns {Object} Reader object with open, readRecords, and close methods
 */
function pipeReaderPlugin(config, streamHelper, journal) {
    var allRecords = [];
    var currentIndex = 0;
    var headers = [];

    return {
        /**
         * Opens the reader and parses the pipe-separated file.
         */
        open: function() {
            // Open the stream with the configured encoding
            var encoding = config.get("encoding") || "UTF-8";
            streamHelper.open(encoding);

            // Read all lines from the stream
            var lines = [];
            var line;
            while ((line = streamHelper.readLine()) !== null) {
                if (line.trim() !== '') {
                    lines.push(line);
                }
            }

            if (lines.length === 0) {
                return;
            }

            // Check if first row contains headers
            var hasHeader = config.has("hasHeader") ? config.get("hasHeader") : true;

            if (hasHeader) {
                // First line is header
                headers = lines[0].split('|').map(function(h) {
                    return h.trim();
                });
                currentIndex = 1;
            } else {
                // Generate column names
                var firstRow = lines[0].split('|');
                headers = firstRow.map(function(_, i) {
                    return 'col' + String(i + 1).padStart(3, '0');
                });
                currentIndex = 0;
            }

            // Parse all data rows
            for (var i = currentIndex; i < lines.length; i++) {
                var values = lines[i].split('|').map(function(v) {
                    return v.trim();
                });

                var record = {};
                for (var j = 0; j < headers.length; j++) {
                    record[headers[j]] = values[j] || '';
                }

                allRecords.push(record);
            }

            currentIndex = 0;

            if (journal && journal.onProgress) {
                journal.onProgress(allRecords.length);
            }
        },

        /**
         * Generator that yields records one at a time.
         */
        readRecords: function*() {
            while (currentIndex < allRecords.length) {
                yield allRecords[currentIndex++];
            }
        },

        /**
         * Closes the reader and cleans up.
         */
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

// Export all tools using the standard pattern
tools.exportAll(exports)
