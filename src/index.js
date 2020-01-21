'use strict';

const { GCSToBigQueryLoader } = require('./gcs-to-bigquery-loader');
const { RawToSchemaGCSFileConverter } = require('./raw-to-schema-gcs-file-converter');

module.exports = {
  GCSToBigQueryLoader: GCSToBigQueryLoader,
  RawToSchemaGCSFileConverter: RawToSchemaGCSFileConverter
};
