'use strict';

const assert = require('assert');
const sinon = require('sinon');

const ndjson = require('ndjson');

const { PassThrough } = require('stream');

const { File, Storage } = require('@google-cloud/storage');
const { LogParser } = require('access-logs-parser');

const { RawToNDJsonGCSFileConverter } = require('../src');

describe('LogParser', () => {

  var consoleLogStub;
  var createWriteStreamStub;
  var converter;

  beforeEach(() => {
    consoleLogStub = sinon.stub(console, 'log');
    createWriteStreamStub = sinon.stub(File.prototype, 'createWriteStream')
      .returns(new PassThrough());
    converter = new RawToNDJsonGCSFileConverter();
  });

  describe('convert', () => {

    it('returns a promise', () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves(['']);

      const file = new Storage().bucket('sourceBucket').file('test.txt');
      const response = converter.convert(file, 'targetBucket');

      assert.strictEqual('[object Promise]', Object.prototype.toString.call(
        response));

      downloadStub.restore();
    });

    it('downloads the source file', () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves(['']);

      const file = new Storage().bucket('sourceBucket').file('test.txt');
      converter.convert(file, 'targetBucket');

      sinon.assert.calledOnce(downloadStub);

      downloadStub.restore();
    });

    it('ignores empty lines', async () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves([
        '\nline 1\n\nline 2\n'
      ]);

      const parseStub = sinon.stub(LogParser.prototype, 'parseTomcatCommonFormat');

      const file = new Storage().bucket('sourceBucket').file('test.txt');
      await converter.convert(file, 'targetBucket');

      sinon.assert.calledTwice(parseStub);

      downloadStub.restore();
      parseStub.restore();
    });

    it('converts json objets to newline delimited json', async () => {
      const downloadStub = sinon.stub(File.prototype, 'download').resolves([
        'line 1\nline 2\nline3'
      ]);

      const parseStub = sinon.stub(LogParser.prototype, 'parseTomcatCommonFormat');

      const fakeStream = ndjson.serialize();
      const streamMock = sinon.mock(fakeStream);
      streamMock.expects('write').thrice();

      const ndjsonStreamStub = sinon.stub(ndjson, 'serialize').returns(fakeStream);

      const file = new Storage().bucket('sourceBucket').file('test.txt');
      await converter.convert(file, 'targetBucket');

      streamMock.verify();

      downloadStub.restore();
      parseStub.restore();
      ndjsonStreamStub.restore();
    });

    afterEach(() => {
      consoleLogStub.restore();
      createWriteStreamStub.restore();
    });

  });

});
