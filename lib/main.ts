import { StreamManager } from "./hls/StreamManager";
import {
  loadConfig,
  RedisJobQueue,
  Job,
  WorkerContext,
  WorkerEventEmitter,
  RedisCache,
  getFile,
  readFile,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";
import Redis from "ioredis";
import { ImageInfo, StreamProps } from "@rewind-media/rewind-protocol";
import "fs/promises";

const log = WorkerLogger.getChildCategory("main");
const config = loadConfig();
const redis = new Redis(config.cacheConfig);
const cache = new RedisCache(redis);
const streamManager = new StreamManager(cache);
const streamJobQueue = new RedisJobQueue<StreamProps, undefined>(
  redis,
  "Stream"
);

const imageJobQueue = new RedisJobQueue<ImageInfo, undefined>(redis, "Image");

streamJobQueue.register(
  async (
    job: Job<StreamProps>,
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
      await stream.run();
    } else {
      context.fail("Failed to initialize stream");
    }
  }
);

imageJobQueue.register(
  async (job: Job<ImageInfo>, context: WorkerContext<undefined>) => {
    context.start();
    try {
      const image = await getFile(job.payload.location).then((it) =>
        readFile(it)
      );
      await cache.putImage(job.payload.id, image, 3600);
      context.success(undefined);
    } catch (e) {
      log.error(`Error processing job ${JSON.stringify(job)}`, e);
      context.fail(JSON.stringify(e));
    }
  }
);
