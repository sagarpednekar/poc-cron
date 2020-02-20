const Queue = require("bull");
const axios = require("axios");
const { connnect, Schema, model } = require("./helper/db");
const moment = require("moment");

const PhotoSchema = new Schema({
  albumId: String,
  id: String,
  photoId: Schema.ObjectId,
  title: String,
  url: String,
  thumbnailUrl: String,
  source: String
});

const PhotoModel = model("Photos", PhotoSchema);

/**
 * Connect Mongo client
 */

connnect();

/**
 *  Initialize CRM and Oracle cron Q here
 *
 */

const crmCronQueue = new Queue("crm-cron", {
  redis: {
    port: 6379,
    host: "127.0.0.1"
  }
});

const oracleCronQueue = new Queue("oracle-cron", {
  redis: {
    port: 6379,
    host: "127.0.0.1"
  }
});

/**
 *  Initialize CRM and Oracle work Q here
 *
 */
const crmWorkQueue = new Queue("crm-work", {
  redis: {
    port: 6379,
    host: "127.0.0.1"
  }
});

const oracleWorkQueue = new Queue("oracle-work", {
  redis: {
    port: 6379,
    host: "127.0.0.1"
  }
});

/**
 *
 * Add JOB to Queue
 *
 */

crmCronQueue.add(
  {
    url: "https://jsonplaceholder.typicode.com/photos",
    source: "crm"
  },
  {
    repeat: {
      // start time for crm cron
      cron: "*/2 * * ? * *"
    }
  }
);

oracleCronQueue.add(
  {
    url: "https://jsonplaceholder.typicode.com/photos",
    source: "oracle"
  },
  {
    repeat: {
      // start time for oracle cron
      cron: "*/2 * * ? * *"
    }
  }
);

/**
 *
 * Start Processing Cron Q
 *
 */

crmCronQueue.process(async job => {
  await cleanQ(crmWorkQueue);
  await cleanQ(oracleWorkQueue);

  console.log("Starting CRM Q");
  crmWorkQueue.add(job.data);
});

oracleCronQueue.process(async job => {
  await cleanQ(crmWorkQueue);
  await cleanQ(oracleWorkQueue);
  console.log("Starting Oracle Q");
  oracleWorkQueue.add(job.data);
});

crmWorkQueue.process(async job => {
  try {
    // fetch records from api

    const response = await axios.get(job.data.url);
    console.log("Response from  CRM --->  ", response.data.length);

    // pick first 1000 records

    let records = response.data.splice(0, 1000);
    records = records.map(photo => {
      return { ...photo, source: "CRM" };
    });
    // records = records.map(photo => { ...photo,source: "CRM"})

    PhotoModel.insertMany(records)
      .then(res => {
        console.log("Result ----> insert in mongo from crm ", res.length);
      })
      .catch(err => console.error(err.stack));

    await crmWorkQueue.add(job.data, { delay: 5000 });
  } catch (error) {
    console.log(error.stack);
  }
});

oracleWorkQueue.process(async job => {
  try {
    // fetch records from api
    const response = await axios.get(job.data.url);
    console.log("Response from oracle ---> ", response.data.length);

    // pick first 1000 records
    let records = response.data.splice(0, 1000);

    // add source field to each record
    records = records.map(photo => {
      // console.log("Photo", photo);
      return { ...photo, source: "ORACLE" };
    });

    // insert 1000 record

    PhotoModel.insertMany(records)
      .then(res => {
        console.log("Result ----> insert in mongo from oracle ", res.length);
      })
      .catch(err => console.error(err.stack));

    oracleWorkQueue.add(job.data, { delay: 5000 });
  } catch (error) {
    console.log(error.stack);
  }
});

/**
 * Checking Work Q status
 *
 *
 */

checkQStatus(crmWorkQueue);
checkQStatus(oracleWorkQueue);

async function cleanQ(queue) {
  try {
    checkQStatusBeforeClean(queue);
    console.log("====================================");
    console.log(`Cleaning ${queue.name} Q `);
    console.log("====================================");
    await queue.pause();
    await queue.clean(0, "active");
    await queue.clean(0, "completed");
    await queue.clean(0, "failed");
    await queue.clean(0, "wait");
    await queue.clean(0, "delayed");
    await queue.empty();
    await queue.resume();
  } catch (error) {
    console.log("Error in cleanredis", error.stack);
  }
}

function checkQStatus(queue) {
  queue.on("resumed", function(job) {
    console.log(`resumed ${queue.name} ---->`, JSON.stringify(job));

    // The queue has been resumed.
  });

  queue.on("cleaned", function(jobs, type) {
    console.log(`cleaned ${queue.name} ---->`, jobs.length, "Jobs with status", type);

    // Old jobs have been cleaned from the crmCronQueue. `jobs` is an array of cleaned
    // jobs, and `type` is the type of jobs cleaned.
  });

  queue.on("paused", function() {
    console.log(`paused ${queue.name} Q`);

    // The crmCronQueue has been paused.
  });
}

function checkQStatusBeforeClean(queue) {
  queue.on("error", function(error) {
    console.log("err --------->", error);
    // An error occured.
  });

  queue.on("waiting", function(jobId) {
    console.log("waiting before clean --- >", jobId);

    // A Job is waiting to be processed as soon as a worker is idling.
  });

  queue.on("active", function(job, jobPromise) {
    console.log("active before clean --- > ", JSON.stringify(job));

    // A job has started. You can use `jobPromise.cancel()`` to abort it.
  });

  queue.on("stalled", function(job) {
    console.log("stalled before clean --- >", JSON.stringify(job));

    // A job has been marked as stalled. This is useful for debugging job
    // workers that crash or pause the event loop.
  });

  queue.on("progress", function(job, progress) {
    console.log("progress before clean --- >", JSON.stringify(job));

    // A job's progress was updated!
  });

  queue.on("completed", function(job, result) {
    console.log("completed before clean --- >", JSON.stringify(job), result);
    // A job successfully completed with a `result`.
  });

  queue.on("failed", function(job, err) {
    console.log(
      "failed before clean --- >",
      JSON.stringify(job),
      "Failed with err",
      err
    );

    // A job failed with reason `err`!
  });
}
