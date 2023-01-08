import { StreamManager } from "./hls/StreamManager.js";
import {
  loadConfig,
  RedisJobQueue,
  Job,
  WorkerEventEmitter,
  RedisCache,
  getFile,
  readFile,
  ClientEventEmitter,
  StreamClientEvents,
  StreamWorkerEvents,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log.js";
import { ImageInfo, StreamProps } from "@rewind-media/rewind-protocol";
import "fs/promises";
import RedisModule from "ioredis";
import { StreamHeartbeat } from "./hls/StreamHeartbeat.js";
// TODO: https://github.com/luin/ioredis/issues/1642
const Redis = RedisModule.default;

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
    context: ClientEventEmitter<undefined, StreamClientEvents>,
    workerEvents: WorkerEventEmitter<StreamWorkerEvents>
  ) => {
    log.info(`Received job ${JSON.stringify(job)}`);
    const stream = streamManager.updateStream(job.payload);
    if (stream) {
      const heartbeat = new StreamHeartbeat(() => {
        streamManager.deleteStream(job.payload.id);
        context.emit("fail", "Failed to heartbeat");
      });
      stream.on("fail", () => {
        context.emit("fail", "Failed to process stream");
        heartbeat.cancel();
      });
      stream.on("succeed", () => {
        context.emit("success", undefined);
        heartbeat.cancel();
      });

      stream.on("init", () => {
        log.info(`Stream ${job.payload.id} initialized.`);
        context.emit("start");
      });

      workerEvents.on("cancel", async () => {
        log.info(`Stream ${job.payload.id} received cancel event`);
        heartbeat.cancel(true);
      });
      workerEvents.on("heartbeat", () => {
        heartbeat.invoke();
      });
      await stream.run();
    } else {
      context.emit("fail", "Failed to initialize stream");
    }
  }
);

imageJobQueue.register(
  async (job: Job<ImageInfo>, context: ClientEventEmitter<undefined>) => {
    try {
      const image = await getFile(job.payload.location).then((it) =>
        readFile(it)
      );
      await cache.putImage(job.payload.id, image, 3600);
      context.emit("success", undefined);
    } catch (e) {
      log.error(`Error processing job ${JSON.stringify(job)}`, e);
      context.emit("fail", JSON.stringify(e));
    }
  }
);
