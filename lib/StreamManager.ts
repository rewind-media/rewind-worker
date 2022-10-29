import { Stream } from "./Stream";
import { JobQueue, RedisCache, Cache } from "@rewind-media/rewind-common";
import { StreamProps } from "@rewind-media/rewind-protocol";

export class StreamManager {
  private readonly streams: Map<string, Stream>;
  private cache: Cache;

  updateStream(monProps: StreamProps): Stream {
    const oldMon = this.streams.get(monProps.id);
    const newMon = new Stream(monProps, this.cache);
    this.streams.set(monProps.id, newMon);
    oldMon?.destroy();
    return newMon;
  }

  async deleteStream(id: string): Promise<void> {
    await this.streams.get(id)?.destroy();
    this.streams.delete(id);
  }

  constructor(cache: Cache) {
    this.streams = new Map();
    this.cache = cache;
  }
}
