'use strict';

const assert = require('assert');
const sinon = require('sinon');

const { Table } = require('@google-cloud/bigquery');

const { GCSToBigQueryLoader } = require('../src');

describe('GCSToBigQueryLoader', () => {

  var consoleLogStub;
  var loader;

  before(() => {
    consoleLogStub = sinon.stub(console, 'log');
  });

  beforeEach(() => {
    loader = new GCSToBigQueryLoader();
  });

  describe('jsonLines', () => {

    it('returns a promise', () => {
      const loadStub = sinon.stub(Table.prototype, 'load').resolves([
        { id: 'Test Job', status: { errors: null } }
      ]);

      const jsonLinesReturn = loader.jsonLines(
        'sourceBucket', 'test.txt', 'targetDataset', 'targetTable');

      assert.strictEqual(Object.prototype.toString.call(jsonLinesReturn),
        '[object Promise]');

      loadStub.restore();
    });

    it('requires the source format as newline delimited json', () => {
      const loadStub = sinon.stub(Table.prototype, 'load').resolves([
        { id: 'Test Job', status: { errors: null } }
      ]);

      loader.jsonLines('sourceBucket', 'test.txt', 'targetDataset', 'targetTable');

      assert.deepStrictEqual(loadStub.getCall(0).args[1], {
        sourceFormat: 'NEWLINE_DELIMITED_JSON'
      });

      loadStub.restore();
    });

    it('rethrows errors that happen when loading data', () => {
      const loadStub = sinon.stub(Table.prototype, 'load').resolves([
        { id: 'Test Job', status: { errors: [{ message: 'Testing an error!' }] } }
      ]);

      assert.rejects(() => loader.jsonLines('sourceBucket', 'test.txt', 'targetDataset',
        'targetTable'), [{ message: 'Testing an error!' }]);

      loadStub.restore();
    });

  });

  after(() => {
    consoleLogStub.restore();
  });

});
