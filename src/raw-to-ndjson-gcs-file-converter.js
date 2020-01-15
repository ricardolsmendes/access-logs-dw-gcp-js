'use strict';

const { Storage } = require('@google-cloud/storage');
const { LogParser } = require('access-logs-parser');

const ndjson = require('ndjson');

class RawToNDJsonGCSFileConverter {

  async convert(file, targetBucket, jsonKeysCase = 'default') {
    console.log(`>> Starting to convert file gs://${file.bucket.name}/${file.name}`);

    console.log(' .   downloading raw contents');
    const sourceContent = await this._loadContents(file);

    const logLines = this._splitStringIntoArray(sourceContent, /\r\n|\r|\n/);

    console.log(` ..  transforming ${logLines.length} lines`);
    const ndjsonStream = this._parseRawIntoNDJson(logLines, jsonKeysCase);

    const targetFile = new Storage()
      .bucket(targetBucket)
      .file(`${file.name.substring(0, file.name.lastIndexOf('.'))}.jsonl`);

    console.log(` ... writing JSON Lines to gs://${targetBucket}/${targetFile.name}`);
    await this._writeContent(ndjsonStream, targetFile);

    console.log('>> DONE!');

    return targetFile;
  }

  /*
   * Load the given file contents into memory.
   */
  async _loadContents(file) {
    const contents = await file.download();
    return contents[0].toString();
  }

  /*
   * Split the given string into an array of substrings,
   * then remove empty elements.
   */
  _splitStringIntoArray(str, splitter) {
    const array = str.split(splitter);
    return array.filter(element => /\S/.test(element));
  }

  /*
   * Parse raw content into JSON objects,
   * then push them as newline delimited JSON to a stream.
   */
  _parseRawIntoNDJson(logLines, jsonKeysCase) {
    const dataStream = ndjson.serialize();
    const logParser = new LogParser();

    logLines.forEach(logLine => {
      const parsedLog = logParser.parseTomcatCommonFormat(logLine, jsonKeysCase);
      dataStream.write(parsedLog);
    });

    dataStream.end();

    return dataStream;
  }

  /*
   * Consume the given stream and write the content to a file.
   */
  _writeContent(inputStream, file) {
    const fileWriteStream = file.createWriteStream({
      resumable: false
    });

    return new Promise((resolve, reject) => {
      inputStream.pipe(fileWriteStream)
        .on('finish', resolve)
        .on('error', reject);
    });
  }

}

module.exports = { RawToNDJsonGCSFileConverter: RawToNDJsonGCSFileConverter };
