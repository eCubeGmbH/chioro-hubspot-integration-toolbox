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

// Export all tools using the standard pattern
tools.exportAll(exports)
