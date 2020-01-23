'use strict';

const assert = require('assert');
const sinon = require('sinon');
const { PassThrough } = require('stream');

const ndjson = require('ndjson');
const { File } = require('@google-cloud/storage');
const { LogParser } = require('access-logs-parser');

const { RawToSchemaGCSFileConverter } = require('../src');

describe('RawToSchemaGCSFileConverter', () => {

  var consoleLogStub;
  var createWriteStreamStub;
  var converter;

  before(() => {
    consoleLogStub = sinon.stub(console, 'log');
  });

  beforeEach(() => {
    createWriteStreamStub = sinon.stub(File.prototype, 'createWriteStream')
      .returns(new PassThrough());
    converter = new RawToSchemaGCSFileConverter();
  });

  describe('jsonLines', () => {

    it('returns a promise', () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves(['']);

      const response = converter.jsonLines('sourceBucket', 'test.txt', 'targetBucket');

      assert.strictEqual(Object.prototype.toString.call(response), '[object Promise]');

      downloadStub.restore();
    });

    it('downloads the source file', () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves(['']);

      converter.jsonLines('sourceBucket', 'test.txt', 'targetBucket');

      sinon.assert.calledOnce(downloadStub);

      downloadStub.restore();
    });

    it('ignores empty lines', async () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves([
        '\nline 1\n\nline 2\n'
      ]);

      const parseStub = sinon.stub(LogParser.prototype, 'parseTomcatCommonFormat');

      await converter.jsonLines('sourceBucket', 'test.txt', 'targetBucket');

      sinon.assert.calledTwice(parseStub);

      downloadStub.restore();
      parseStub.restore();
    });

    it('allows generating JSON with keys in special case', async () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves([
        'line 1'
      ]);

      const parseStub = sinon.stub(LogParser.prototype, 'parseTomcatCommonFormat');

      await converter.jsonLines('sourceBucket', 'test.txt', 'targetBucket', 'snake');

      sinon.assert.calledWith(parseStub, 'line 1', 'snake');

      downloadStub.restore();
      parseStub.restore();
    });

    it('converts JSON objects to newline delimited JSON', async () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves([
        'line 1\nline 2\nline 3'
      ]);

      const parseStub = sinon.stub(LogParser.prototype, 'parseTomcatCommonFormat');

      const fakeStream = ndjson.serialize();
      const writeStub = sinon.stub(fakeStream, 'write');
      const ndjsonSerializeStub = sinon.stub(ndjson, 'serialize').returns(fakeStream);

      await converter.jsonLines('sourceBucket', 'test.txt', 'targetBucket');

      sinon.assert.calledThrice(writeStub);

      downloadStub.restore();
      parseStub.restore();
      ndjsonSerializeStub.restore();
    });

    afterEach(() => {
      createWriteStreamStub.restore();
    });

    after(() => {
      consoleLogStub.restore();
    });

  });

});
