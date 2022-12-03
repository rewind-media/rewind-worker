import { Cache } from "@rewind-media/rewind-common";
import { SegmentObject } from "mp4frag";

export class StreamDataHelper {
  private readonly cache: Cache;
  private setSegment: boolean = false;
  private setInitMp4: boolean = false;
  private runInit: boolean = true;
  private readonly onInit: () => void;
  private readonly expiration: Date;
  private readonly streamId: string;

  constructor(
    cache: Cache,
    streamId: string,
    expiration: Date,
    onInit: () => void
  ) {
    this.cache = cache;
    this.onInit = onInit;
    this.expiration = expiration;
    this.streamId = streamId;
  }

  async putSegment(segment: SegmentObject) {
    if (segment.segment) {
      await this.cache.putSegmentM4s(
        this.streamId,
        segment.sequence,
        segment.segment,
        this.expiration
      );
      this.setSegment = true;
      await this.runInitOnceIfReady();
    }
  }

  async putInitMp4(initMp4: Buffer) {
    await this.cache.putInitMp4(this.streamId, initMp4, this.expiration);
    this.setInitMp4 = true;
    await this.runInitOnceIfReady();
  }

  private async runInitOnceIfReady() {
    if (this.runInit && this.setInitMp4 && this.setSegment) {
      this.runInit = false;
      await this.onInit();
    }
  }
}
