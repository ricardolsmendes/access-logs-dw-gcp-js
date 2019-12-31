'use strict';

const { LogParser } = require('access-logs-parser');
const { Storage } = require('@google-cloud/storage');

const ndjson = require('ndjson');

class RawToNDJsonGCSFileConverter {

  constructor() {
    this._storage = new Storage();
  }

  async convert(sourceBucket, sourceFilename, targetBucket) {
    console.log(`Starting to convert gs://${sourceBucket}/${sourceFilename}`);

    const sourceContent = await this._loadSourceContent(sourceBucket, sourceFilename);
    console.log(' .   source content downloaded');

    const logLines = this._splitStringIntoArray(sourceContent, /\r\n|\n\r|\r|\n/);

    console.log(` ..  converting ${logLines.length} lines`);
    const dataStream = this._parseRawIntoNDJson(logLines);

    const filenamePrefix = `${sourceFilename.substring(0, sourceFilename.lastIndexOf('.'))}`;
    const targetFilename = `${filenamePrefix}.jsonl`;
    console.log(` ... writing converted content into gs://${targetBucket}/${targetFilename}`);
    await this._writeTargetFile(targetBucket, targetFilename, dataStream);

    console.log('DONE!');
  }

  /*
   * Load raw content from the original log file.
   */
  async _loadSourceContent(bucket, filename) {
    const downloadedChunks = await this._storage
      .bucket(bucket)
      .file(filename)
      .download();

    const sourceContent = ''.concat(downloadedChunks);

    return sourceContent;
  }

  /*
   * Split the given string into an array of substrings and remove empty elements.
   */
  _splitStringIntoArray(str, splitter) {
    const array = str.split(splitter);
    return array.filter(element => /\S/.test(element));
  }

  /*
   * Parse raw content into JSON objects and push them to a stream.
   */
  _parseRawIntoNDJson(logLines) {
    const dataStream = ndjson.serialize();
    const logParser = new LogParser();

    logLines.forEach(function(logLine) {
      const parsedLog = logParser.parseTomcatCommonFormat(logLine);
      dataStream.write(parsedLog);
    });

    return dataStream;
  }

  /*
   * Write data to the target file using the given stream.
   */
  async _writeTargetFile(bucket, filename, dataStream) {
    const targetFile = this._storage
      .bucket(bucket)
      .file(filename);

    await dataStream.pipe(targetFile.createWriteStream({
      resumable: false
    }));

    dataStream.end();
  }

}

module.exports = { RawToNDJsonGCSFileConverter: RawToNDJsonGCSFileConverter };
