import { StreamManager } from "./StreamManager";
import {
  loadConfig,
  RedisJobQueue,
  Job,
  WorkerContext,
  WorkerEventEmitter,
  RedisCache,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";
import Redis from "ioredis";
import { StreamProps } from "@rewind-media/rewind-protocol";

const log = WorkerLogger.getChildCategory("main");
const config = loadConfig();
const redis = new Redis(config.cacheConfig);
const cache = new RedisCache(redis);
const streamManager = new StreamManager(cache);
const streamJobQueue = new RedisJobQueue<StreamProps, undefined>(
  redis,
  "Stream"
);

// TODO stream cleanup queue
streamJobQueue.register(
  async (
    job: Job<StreamProps, undefined>,
    context: WorkerContext<undefined>,
    workerEvents: WorkerEventEmitter
  ) => {
    log.info(`Received job ${JSON.stringify(job)}`);
    const stream = streamManager.updateStream(job.payload);
    if (stream) {
      stream.on("fail", () => {
        context.fail("Failed to process stream");
      });
      stream.on("succeed", () => {
        context.success(undefined);
      });

      stream.on("init", () => {
        log.info(`Stream ${job.payload.id} initialized.`);
        context.start();
      });

      workerEvents.on("cancel", async () => {
        log.info(`Stream ${job.payload.id} received cancel event`);
        await streamManager.deleteStream(job.payload.id);
      });
      stream.run();
    } else {
      context.fail("Failed to initialize stream");
    }
  }
);
