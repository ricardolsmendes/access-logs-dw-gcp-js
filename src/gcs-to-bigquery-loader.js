'use strict';

const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

/**
 * Imports a GCS file into a BigQUery Table.
 */
class GCSToBigQueryLoader {

  /**
   * Appends data to an existing table.
   */
  async jsonLines(bucketName, filename, datasetId, tableId) {

    // Configure the load job. For full list of options, see:
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad.
    const metadata = {
      sourceFormat: 'NEWLINE_DELIMITED_JSON'
    };

    // Load data from a Google Cloud Storage file into the Table
    const sourceFile = new Storage()
      .bucket(bucketName)
      .file(filename);

    const [job] = await new BigQuery()
      .dataset(datasetId)
      .table(tableId)
      .load(sourceFile, metadata);

    console.log(`Job ${job.id} completed.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }
  }

}

module.exports = { GCSToBigQueryLoader: GCSToBigQueryLoader };
