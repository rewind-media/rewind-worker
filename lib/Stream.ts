import { FFmpegCommand } from "fessonia";
import { ChildProcess } from "child_process";
import { PassThrough } from "stream";
import Mp4Frag, { SegmentObject } from "mp4frag";
import { FfProbeInfo } from "./util/ffprobe";
import {
  Cache,
  StreamMetadata,
  StreamSegmentMetadata,
} from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";
import { StreamProps } from "@rewind-media/rewind-protocol";
import EventEmitter from "events";
import { first } from "lodash";
import { FFProbeStream } from "ffprobe";

const ff = require("fessonia")({
  debug: true,
  log_warnings: true,
});

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
  private processedSecs: number = 0;
  private segments: StreamSegmentMetadata[] = [];
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
  private _init: Buffer | undefined;
  private cache: Cache;
  private hasInit: boolean = false;
  private hasSegment: boolean = false;

  constructor(props: StreamProps, cache: Cache) {
    super();
    this.cache = cache;
    this.props = props;
    this.sourcePipe = new PassThrough();
    this.expirationDate = new Date(
      Math.ceil(Date.now() + props.duration * 1000)
    );
    this.m4f = new Mp4Frag();

    const input = new ff.FFmpegInput(props.mediaInfo.path, {
      loglevel: "quiet",
      probesize: "1000000",
      analyzeduration: "1000000",
      ss: props.startOffset.toString(),
      // reorder_queue_size: '5'
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
      ac: "2", // TODO makes audio streams stereo only because 5.1 doesn't work on android hls.js for some reason
      movflags: "+frag_keyframe+empty_moov+default_base_moof",
    });
    this.cmd = new ff.FFmpegCommand();

    this.cmd.addInput(input);
    this.cmd.addOutput(output);
  }

  private static isMp4CopyCompatible(info: FfProbeInfo) {
    return !first(
      info.streams.filter(
        (it) =>
          it.codec_name &&
          Stream.mp4AudioCodes
            .concat(Stream.mp4VideoCodecs)
            .indexOf(it.codec_name.toLowerCase()) === -1
      )
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
    this.segments = [];

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
    const processOut = this.process.stdout;
    this.hasSegment = false;
    this.hasInit = false;
    if (!processOut) {
      setTimeout(() => {
        this.process?.kill();
      }, 100);
    } else {
      processOut
        .pipe(this.m4f)
        .on("segment", async (segment: SegmentObject) => {
          // processOut?.pause();
          const buffer = segment.segment;
          if (!buffer) {
            log.warn(`Got empty segment: ${JSON.stringify(segment)}`);
            return;
          }

          this.processedSecs += segment.duration;
          this.segments.push({
            index: segment.sequence,
            duration: segment.duration,
          });
          this.segments.sort((a, b) => a.index - b.index);
          return Promise.all([
            this.cache.putStreamMetadata(
              this.props.id,
              this.mkStreamMetadata(),
              this.expirationDate
            ),
            this.cache.putSegmentM4s(
              this.props.id,
              segment.sequence,
              buffer,
              this.expirationDate
            ),
          ]).then(() => {
            // processOut?.resume();
            if (!this.hasSegment) {
              this.hasSegment = true;
              if (this.hasInit) {
                this.emit("init");
              }
            }

            log.info(
              `Stream ${this.props.id}, Segment ${segment.sequence}, ${
                this.processedSecs + this.props.startOffset
              }/${this.props.duration} seconds`
            );
          });
        })
        // TODO specify the emitted type in @types/mp4frag
        .on(
          "initialized",
          async (init: {
            mime: string;
            initialization: Buffer;
            m3u8: string;
          }) => {
            if (!this.hasInit && init.initialization) {
              await this.cache.putInitMp4(
                this.props.id,
                init.initialization,
                this.expirationDate
              );
              this.hasInit = true;
              if (this.hasSegment) {
                this.emit("init");
              }
            }
          }
        )
        .on("error", (reason) => {
          log.error(`Stream ${this.props.id} failed`, reason);
        });
    }
  }

  async _destroy(): Promise<void> {
    try {
      this.process?.removeAllListeners();
      this.process?.stdout?.removeAllListeners();
      this.process?.stdout?.destroy();
      this.process?.kill();
      this.removeAllListeners();
      this.cmd.removeAllListeners();
      await Promise.all([
        this.cache.delStreamMetadata(this.props.id),
        this.cache.delInitMp4(this.props.id),
        ...this.segments.map((seg) =>
          this.cache.delSegmentM4s(this.props.id, seg.index)
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
    return this._destroy();
  }

  private mkStreamMetadata(complete: boolean = false): StreamMetadata {
    return {
      segments: this.segments,
      complete: complete,
    };
  }

  private async endStream(success: boolean) {
    await this.cache.putStreamMetadata(
      this.props.id,
      this.mkStreamMetadata(true),
      this.expirationDate
    );
    if (success) {
      this.emit("fail");
    } else {
      this.emit("succeed");
    }
  }
}
