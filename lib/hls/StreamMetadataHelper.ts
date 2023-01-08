import {
  Cache,
  Mime,
  StreamMetadata,
  StreamSegmentMetadata,
} from "@rewind-media/rewind-common";
import { SegmentObject } from "mp4frag";
import { Duration } from "durr";

export class StreamMetadataHelper {
  private _processedSecs: number = 0;
  private mime?: Mime;
  private segmentMetadata: StreamSegmentMetadata[] = [];
  private complete: boolean = false;
  private subtitles?: string;
  private cache: Cache;
  private readonly streamId: string;
  private readonly durationSecs: number;

  constructor(streamId: string, durationSecs: number, cache: Cache) {
    this.streamId = streamId;
    this.cache = cache;
    this.durationSecs = durationSecs;
  }

  async putSegment(segment: SegmentObject) {
    this.segmentMetadata.push({
      duration: segment.duration,
      index: segment.sequence,
    });
    this._processedSecs += segment.duration;
    await this.updateMetadata();
  }

  get processedSecs(): number {
    return this._processedSecs;
  }

  async setMime(mime: Mime) {
    this.mime = mime;
    await this.updateMetadata();
  }

  async setSubtitles(subtitles: string) {
    this.subtitles = subtitles;
    await this.updateMetadata();
  }

  async setComplete() {
    this.complete = true;
    await this.updateMetadata();
  }

  getStreamMetadata(): StreamMetadata | undefined {
    return this.mime
      ? {
          segments: this.segmentMetadata,
          complete: this.complete,
          subtitles: this.subtitles,
          mime: this.mime,
          processedSecs: this.processedSecs,
          totalDurationSecs: this.durationSecs,
        }
      : undefined;
  }

  private async updateMetadata() {
    const metadata = this.getStreamMetadata();
    if (metadata) {
      await this.cache.putStreamMetadata(
        this.streamId,
        metadata,
        Duration.seconds(15).after()
      );
    }
  }
}
