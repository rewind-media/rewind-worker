import { Cache } from "@rewind-media/rewind-common";
import { SegmentObject } from "mp4frag";
import { Duration } from "durr";

export class StreamDataHelper {
  private readonly cache: Cache;
  private setSegment: boolean = false;
  private setInitMp4: boolean = false;
  private runInit: boolean = true;
  private readonly onInit: () => void;
  private readonly streamId: string;

  constructor(cache: Cache, streamId: string, onInit: () => void) {
    this.cache = cache;
    this.onInit = onInit;
    this.streamId = streamId;
  }

  async putSegment(segment: SegmentObject) {
    if (segment.segment) {
      await this.cache.putSegmentM4s(
        this.streamId,
        segment.sequence,
        segment.segment,
        Duration.seconds(15).after()
      );
      this.setSegment = true;
      await this.runInitOnceIfReady();
    }
  }

  async putInitMp4(initMp4: Buffer) {
    await this.cache.putInitMp4(
      this.streamId,
      initMp4,
      Duration.seconds(15).after()
    );
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
