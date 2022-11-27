import { FFmpegCommand } from "fessonia";
import { ChildProcess } from "child_process";
import { PassThrough, Readable, Writable } from "stream";
import Mp4Frag, { SegmentObject } from "mp4frag";
import { FfProbeInfo } from "./util/ffprobe";
import {
  Cache,
  getFile,
  Mime,
  readFile,
  StreamMetadata,
  StreamSegmentMetadata,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";
import { isFileLocation, StreamProps } from "@rewind-media/rewind-protocol";
import EventEmitter from "events";
import { FFProbeStream } from "ffprobe";

const ff = require("fessonia")({
  debug: true,
  log_warnings: true,
});

class StreamDataHelper {
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

class StreamMetadataHelper {
  private _processedSecs: number = 0;
  private mime?: Mime;
  private segmentMetadata: StreamSegmentMetadata[] = [];
  private complete: boolean = false;
  private subtitles?: string;
  private cache: Cache;
  private readonly streamId: string;
  private readonly expirationDate: Date;
  private readonly durationSecs: number;
  constructor(
    streamId: string,
    durationSecs: number,
    cache: Cache,
    expirationDate: Date
  ) {
    this.streamId = streamId;
    this.cache = cache;
    this.expirationDate = expirationDate;
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
        this.expirationDate
      );
    }
  }
}

export interface StreamEvents {
  init: () => void;
  succeed: () => void;
  fail: () => void;
  destroy: () => void;
}
export declare interface StreamEventEmitter extends EventEmitter {
  on<U extends keyof StreamEvents>(event: U, listener: StreamEvents[U]): this;

  emit<U extends keyof StreamEvents>(
    event: U,
    ...args: Parameters<StreamEvents[U]>
  ): boolean;
}
export class StreamEventEmitter extends EventEmitter {
  constructor() {
    super();
  }
}

const log = WorkerLogger.getChildCategory("Stream");

// TODO copy streams types that are compatible with mp4
// https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Video_codecs
// https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Audio_codecs
// TODO support audio only streams
export class Stream extends StreamEventEmitter {
  private props: StreamProps;
  private process?: ChildProcess;
  private cmd: FFmpegCommand;
  private sourcePipe: PassThrough;
  private m4f: Mp4Frag;
  readonly expirationDate: Date;

  private static readonly mp4VideoCodecs = [
    "av1",
    "h264",
    "h265",
    "mp4v-es",
    "mpeg-2",
    "vp9",
  ];

  private static readonly mp4AudioCodes = [
    "aac",
    "alac",
    "flac",
    "mp3",
    "opus",
  ];

  private static readonly mp4SubtitleCodecs = ["sbtt"];
  private cache: Cache;
  private hasInit: boolean = false;
  private hasSegment: boolean = false;
  private fileStream?: Readable;
  private metadataHelper: StreamMetadataHelper;
  private dataHelper: StreamDataHelper;

  constructor(props: StreamProps, cache: Cache) {
    super();
    this.cache = cache;
    this.props = props;
    this.sourcePipe = new PassThrough();
    this.expirationDate = new Date(
      Math.ceil(Date.now() + props.duration * 1000)
    );
    this.m4f = new Mp4Frag();
    //const inputStart = props.startOffset - 30;
    const input = new ff.FFmpegInput("pipe:0", {
      loglevel: "quiet",
      probesize: "1000000",
      analyzeduration: "1000000",
      accurate_seek: "-accurate_seek",
      // TODO seek to preceding keyframe instead, and remove ss from output
      // seek_timestamp: "-seek_timestamp",
      // ...(inputStart > 0 ? { ss: inputStart } : {}),
    });

    //TODO a crappy way of limiting bandwidth - this should be configurable and autoscaling
    const limiters = {
      // crf: "24", // TODO
      // bufsize: "64k",
    };

    const output = new ff.FFmpegOutput(`pipe:1`, {
      ...Stream.mkAllStreamOptions(props.mediaInfo.info),
      ...limiters,
      f: "mp4",
      // an: '',
      // hls_time: "10",
      // hls_allow_cache: "0",
      // hls_flags: "+delete_segments+omit_endlist",
      // hls_playlist_type: "vod"
      ss: props.startOffset.toString(),
      ac: "2", // TODO makes audio streams stereo only because 5.1 doesn't work on android hls.js for some reason
      movflags: "+frag_keyframe+empty_moov+default_base_moof",
    });
    this.cmd = new ff.FFmpegCommand();

    this.cmd.addInput(input);
    this.cmd.addOutput(output);

    this.metadataHelper = new StreamMetadataHelper(
      this.props.id,
      this.props.duration,
      this.cache,
      this.expirationDate
    );
    this.dataHelper = new StreamDataHelper(
      this.cache,
      this.props.id,
      this.expirationDate,
      () => {
        this.emit("init");
      }
    );
  }

  private static mkAllStreamOptions(info: FfProbeInfo): {
    [p: string]: string;
  } {
    return Object.assign(
      {},
      ...info.streams.map((it) => this.mkStreamOptions(it) ?? {})
    );
  }
  private static mkStreamOptions(
    stream: FFProbeStream
  ): { [p: string]: string } | undefined {
    if (stream.codec_type == "video") {
      const key = `c:v:${stream.index}`;
      if (
        stream.codec_name &&
        this.mp4VideoCodecs.includes(stream.codec_name)
      ) {
        return { [key]: "copy" };
      } else {
        return { [key]: "h264" };
      }
    } else if (stream.codec_type == "audio") {
      const key = `c:a:${stream.index}`;
      if (stream.codec_name && this.mp4AudioCodes.includes(stream.codec_name)) {
        return { [key]: "copy" };
      } else {
        return { [key]: "aac" };
      }
    } else if (
      // @ts-ignore
      // TODO fix the typescript values for stream.codec_type
      stream.codec_type == "subtitle"
    ) {
      const key = `c:s:${stream.index}`;
      if (
        stream.codec_name &&
        this.mp4SubtitleCodecs.includes(stream.codec_name)
      ) {
        return { [key]: "copy" };
      } else {
        return { [key]: "sbtt" };
      }
    } else if (stream.codec_type == "images") {
      log.error(
        `Stream codec type 'images' with name ${stream.codec_name} is not supported`
      );
      return undefined;
    } else {
      log.error(
        `Unknown stream codec type '${stream.codec_type}' with codec '${stream.codec_name}'`
      );
      return undefined;
    }
  }

  run() {
    log.info(`Stream ${this.props.id} running FFMPEG: ${this.cmd.toString()}`);
    this.m4f.resetCache();
    this.process = this.cmd.spawn();
    this.fileStream?.destroy();
    // const mp4BoxFile = Mp4Box.createFile();
    // mp4BoxFile.onReady = async (it) => {
    //   await this.metadataHelper.setCodecs(it.tracks.map((a) => a.codec));
    // };

    this.cmd
      .on("exit", async (code, signal) => {
        await this.endStream(code !== 0);
      })
      .on("close", async (code, signal) => {
        await this.endStream(code !== 0);
      })
      .on("disconnect", async () => {
        // TODO do anything?
      })
      .on("error", (err) => {
        log.error(`Stream ${this.props.id} failed`, err);
        this.emit("fail");
      });

    this.process
      .on("exit", async (code, signal) => {
        await this.endStream(code !== 0);
      })
      .on("close", async (code, signal) => {
        await this.endStream(code !== 0);
      })
      .on("disconnect", async () => {
        // TODO do anything?
      })
      .on("error", async (err) => {
        log.error(`Stream ${this.props.id} failed`, err);
        await this.endStream(false);
      });
    const processIn = this.process.stdin;
    const processOut = this.process.stdout;
    if (processIn) {
    }
    this.hasSegment = false;
    this.hasInit = false;
    if (!processOut || !processIn) {
      setTimeout(() => {
        this.process?.kill();
      }, 100);
    } else {
      getFile(this.props.mediaInfo.location).then(async (readable) => {
        if (this.props.subtitle) {
          (isFileLocation(this.props.subtitle)
            ? this.handleSubtitles(
                await getFile(this.props.subtitle),
                this.props.startOffset
              )
            : this.handleSubtitles(
                readable,
                this.props.startOffset,
                this.props.subtitle.index
              )
          ).then((subtitles) => this.metadataHelper.setSubtitles(subtitles));
        }

        this.fileStream = readable;
        const errorHandler = (e: any) => {
          log.error(
            `Error reading file from ${JSON.stringify(
              this.props.mediaInfo.location
            )}`,
            e
          );
          processIn.end();
        };
        readable.pipe(processIn).on("error", errorHandler);
        readable.on("error", errorHandler);
      });

      processOut
        .pipe(this.m4f)
        .on("segment", async (segment: SegmentObject) => {
          const buffer = segment.segment;
          if (!buffer) {
            log.warn(`Got empty segment: ${JSON.stringify(segment)}`);
            return;
          }
          await Promise.all([
            this.metadataHelper.putSegment(segment),
            this.dataHelper.putSegment(segment),
          ]);

          log.info(
            `Stream ${this.props.id}, Segment ${segment.sequence}, ${
              this.metadataHelper.processedSecs + this.props.startOffset
            }/${this.props.duration} seconds`
          );
        })
        // TODO specify the emitted type in @types/mp4frag
        .on(
          "initialized",
          async (init: {
            mime: string;
            initialization: Buffer;
            m3u8: string;
          }) => {
            await Promise.all([
              await this.dataHelper.putInitMp4(init.initialization),
              await this.metadataHelper.setMime(
                this.parseMp4FragMime(init.mime)
              ),
            ]);
            log.info(`MIME: ${init.mime}`);
          }
        )
        .on("error", (reason) => {
          log.error(`Stream ${this.props.id} failed`, reason);
        });
    }
  }

  async _destroy(): Promise<void> {
    try {
      this.fileStream?.destroy();
      this.process?.removeAllListeners();
      this.process?.stdout?.removeAllListeners();
      this.process?.stdout?.destroy();
      this.process?.kill();
      this.removeAllListeners();
      this.cmd.removeAllListeners();
      await Promise.all([
        this.cache.delStreamMetadata(this.props.id),
        this.cache.delInitMp4(this.props.id),
        ...(this.metadataHelper.getStreamMetadata()?.segments ?? []).map(
          (seg) => this.cache.delSegmentM4s(this.props.id, seg.index)
        ),
      ]);
    } catch (e) {
      log.error(`Error killing stream ${this.props.id}`, e);
    }
  }

  destroy(): Promise<void> {
    // Do a second pass in 30 seconds in case anything propagated in that time
    // The first pass is to free RAM, the second to try and catch the rest, and there's a TTL in the worst case.
    log.info(`Killing stream ${this.props.id}`);
    setTimeout(() => this._destroy(), 30000);
    return this._destroy().then(() =>
      log.info(`Killed Stream ${this.props.id}`)
    );
  }

  private async endStream(success: boolean) {
    await this.metadataHelper.setComplete();
    if (success) {
      this.emit("fail");
    } else {
      this.emit("succeed");
    }
  }

  private parseMp4FragMime(mime: string): Mime {
    const [typeStr, codecStr] = mime.split(";").map((it) => it.trim());
    const codecs = codecStr
      .replace("codecs=", "")
      .replaceAll('"', "")
      .split(",")
      .map((it) => it.trim());
    return {
      mimeType: typeStr,
      codecs: codecs,
    };
  }

  private async handleSubtitles(
    readable: Readable,
    startOffset: number,
    subtitleTrack: number = 0
  ) {
    const input = new ff.FFmpegInput("pipe:0", {});
    const output = new ff.FFmpegOutput("pipe:1", {
      f: "webvtt",
      ss: startOffset.toString(),
      // map: `${subtitleTrack}:s:0`, //TODO get embedded subtitles working
    });

    const cmd = new ff.FFmpegCommand();
    cmd.addInput(input);
    cmd.addOutput(output);
    log.info(`Extracting subtitles with: ${cmd.toString()}`);
    const process = cmd.spawn();
    readable.pipe(process.stdin);
    const subs = (await readFile(process.stdout)).toString("utf8");
    log.debug(`Subtitles: ${subs}`);
    return subs;
    // return (await readFile(readable)).toString("utf8");
  }
}
