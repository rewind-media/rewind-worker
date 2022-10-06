import { StreamManager } from "./StreamManager";
import {
  Database,
  loadConfig,
  mkMongoDatabase,
  mkRedisCache,
  Cache,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";

const log = WorkerLogger.getChildCategory("main");
const config = loadConfig();
mkMongoDatabase(config.databaseConfig).then((db: Database) => {
  mkRedisCache(config.cacheConfig).then(async (cache: Cache) => {
    const streamManager = new StreamManager(cache);
    const jobQueue = cache.getJobQueue("JobQueue");
    while (true) {
      const job = await jobQueue.listen();
      log.info(`Got job: ${JSON.stringify(job)}`);

      await jobQueue
        .monitor(job.id)
        .then((eventEmitter) => {
          log.info(`Monitoring Job ${job.id}`);
          eventEmitter.on("cancel", async () => {
            log.info(`Got cancel event for ${job.id}`);
            await streamManager.deleteStream(job.payload.id);
          });
          return jobQueue.current(job.id);
        })
        .then(async (state) => {
          switch (state) {
            case "submit":
              log.info(`Starting submitted job ${job.id}`);
              return (
                streamManager
                  .updateStream(job.payload)
                  //TODO emit JobState.SUCCESS.
                  .then((stream) => {
                    if (stream) {
                      stream.on("fail", () =>
                        jobQueue.update(
                          job.id,
                          "fail",
                          stream?.expirationDate ?? nowPlusOneHour()
                        )
                      );
                      stream.on("succeed", () =>
                        jobQueue.update(
                          job.id,
                          "succeed",
                          stream?.expirationDate ?? nowPlusOneHour()
                        )
                      );
                    }
                    jobQueue.update(
                      job.id,
                      stream ? "start" : "fail",
                      stream?.expirationDate ?? nowPlusOneHour()
                    );
                  })
              );
            case "cancel":
            case "fail":
              log.warn(
                `Job ${job.id} has failed, possibly before emitter was set up.`
              );
              return;
            case "succeed":
              return;
          }
        });
    }
  });
});

function nowPlusOneHour(): Date {
  return new Date(Date.now() + 3600000);
}
