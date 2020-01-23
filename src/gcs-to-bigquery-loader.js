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
  async jsonLines(sourceBucketName, sourceFileName, targetDatasetId, targetTableId) {

    console.log(`>> Starting to load file gs://${sourceBucketName}/${sourceFileName}`);
    const sourceFile = new Storage()
      .bucket(sourceBucketName)
      .file(sourceFileName);

    // Configure the load job. For full list of options, see:
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad.
    const metadata = {
      sourceFormat: 'NEWLINE_DELIMITED_JSON'
    };

    // Load data from a Google Cloud Storage file into the table
    const table = new BigQuery()
      .dataset(targetDatasetId)
      .table(targetTableId);

    console.log(` . writing content into ${targetDatasetId}.${targetTableId}`);
    const [job] = await table.load(sourceFile, metadata);

    console.log(`>> JOB ${job.id} COMPLETED!`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }

    return table;
  }

}

module.exports = { GCSToBigQueryLoader: GCSToBigQueryLoader };
