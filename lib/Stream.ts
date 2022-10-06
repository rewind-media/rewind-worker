import { FFmpegCommand } from "fessonia";
import { ChildProcess } from "child_process";
import { PassThrough } from "stream";
import Mp4Frag, { SegmentObject } from "mp4frag";
import { FfProbeInfo } from "./util/ffprobe";
import { Cache } from "@rewind-media/rewind-common";
import { WorkerLogger } from "./log";
import { StreamProps } from "@rewind-media/rewind-protocol";
import EventEmitter from "events";
import _ from "lodash";

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

interface SegmentInfo {
  index: number;
  duration: number;
}

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
  private segments: SegmentInfo[] = [];
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
  private _init: Buffer | undefined;
  private cache: Cache;

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
      crf: "24", // TODO
      bufsize: "64k",
    };

    const output = new ff.FFmpegOutput(`pipe:1`, {
      ...(Stream.isMp4CopyCompatible(props.mediaInfo.info) && { c: "copy" }),
      ...limiters,
      f: "mp4",
      // an: '',
      // hls_time: "10",
      // hls_allow_cache: "0",
      // hls_flags: "+delete_segments+omit_endlist",
      // hls_playlist_type: "vod"
      movflags: "+frag_keyframe+empty_moov+default_base_moof",
    });
    this.cmd = new ff.FFmpegCommand();

    this.cmd.addInput(input);
    this.cmd.addOutput(output);
  }

  private static isMp4CopyCompatible(info: FfProbeInfo) {
    return !_.first(
      info.streams.filter(
        (it) =>
          it.codec_name &&
          Stream.mp4AudioCodes
            .concat(Stream.mp4VideoCodecs)
            .indexOf(it.codec_name.toLowerCase()) === -1
      )
    );
  }

  run(): Promise<Stream | undefined> {
    log.info(`Stream ${this.props.id} running FFMPEG: ${this.cmd.toString()}`);
    this.m4f.resetCache();
    this.process = this.cmd.spawn();
    this.segments = [];

    this.cmd
      .on("exit", async (code, signal) => {
        if (code !== 0) {
          this.emit("fail");
        } else {
          this.emit("succeed");
        }
      })
      .on("close", async (code, signal) => {
        if (code !== 0) {
          this.emit("fail");
        } else {
          this.emit("succeed");
        }
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
        if (code !== 0) {
          this.emit("fail");
        } else {
          this.emit("succeed");
        }
      })
      .on("close", async (code, signal) => {
        if (code !== 0) {
          this.emit("fail");
        } else {
          this.emit("succeed");
        }
      })
      .on("disconnect", async () => {
        // TODO do anything?
      })
      .on("error", (err) => {
        log.error(`Stream ${this.props.id} failed`, err);
        this.emit("fail");
      });
    const processOut = this.process.stdout;
    return new Promise<Stream>((resolve, reject) => {
      let hasSegment = false;
      let hasInit = false;
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
              this.cache.putM3u8(
                this.props.id,
                this.mkInitM3u8(),
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
              if (!hasSegment) {
                hasSegment = true;
                if (hasInit) {
                  resolve(this);
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
              if (!hasInit && init.initialization) {
                await this.cache.putInitMp4(
                  this.props.id,
                  init.initialization,
                  this.expirationDate
                );
                hasInit = true;
                if (hasSegment) {
                  resolve(this);
                  this.emit("init");
                }
              }
            }
          )
          .on("error", (reason) => {
            log.error(`Stream ${this.props.id} failed`, reason);
          });
      }
    }).catch((reason) => {
      log.error(
        `Stream ${JSON.stringify(this.props)} failed with reason ${reason}`
      );
      return undefined;
    });
  }

  async _destroy(): Promise<void> {
    try {
      this.process?.removeAllListeners();
      this.process?.stdout?.removeAllListeners();
      this.process?.kill();
      await Promise.all([
        this.cache.delM3u8(this.props.id),
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

  private mkInitM3u8() {
    return (
      "#EXTM3U\n" +
      "#EXT-X-VERSION:7\n" +
      "#EXT-X-TARGETDURATION:5\n" +
      "#EXT-X-MEDIA-SEQUENCE:0\n" +
      '#EXT-X-MAP:URI="init-stream.mp4"\n' +
      this.segments
        .map((seg) => `#EXTINF:${seg.duration},\n${seg.index}.m4s\n`)
        .join("")
    );
  }
}
