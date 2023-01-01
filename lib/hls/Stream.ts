import { FFmpegCommand } from "fessonia";
import { ChildProcess } from "child_process";
import { Readable, Writable } from "node:stream";
import Mp4Frag, { SegmentObject } from "mp4frag";
import { FfProbeInfo } from "../util/ffprobe.js";
import { Cache, getFile, Mime, readFile } from "@rewind-media/rewind-common";
import { WorkerLogger } from "../log.js";
import { isFileLocation, StreamProps } from "@rewind-media/rewind-protocol";
import { FFProbeStream } from "ffprobe";
import { StreamDataHelper } from "./StreamDataHelper.js";
import { StreamMetadataHelper } from "./StreamMetadataHelper.js";
import { StreamEventEmitter } from "./models.js";
import { Duration } from "durr";
import fessonia from "fessonia";

const ff = fessonia({
  debug: true,
  log_warnings: true,
});

const log = WorkerLogger.getChildCategory("Stream");

// TODO copy streams types that are compatible with mp4
// https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Video_codecs
// https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Audio_codecs
// TODO support audio only streams
export class Stream extends StreamEventEmitter {
  private readonly cache: Cache;
  private readonly cmd: FFmpegCommand;
  private readonly expirationDate: Date;
  private readonly props: StreamProps;
  private readonly metadataHelper: StreamMetadataHelper;
  private readonly m4f: Mp4Frag;
  private readonly dataHelper: StreamDataHelper;
  private process?: ChildProcess;
  private fileStream?: Readable;

  constructor(props: StreamProps, cache: Cache) {
    super();
    this.cache = cache;
    this.props = props;
    this.expirationDate = Duration.seconds(props.duration * 3).after();
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

  async run() {
    log.info(`Stream ${this.props.id} running FFMPEG: ${this.cmd.toString()}`);
    this.fileStream?.destroy();
    this.m4f.resetCache();
    this.process = this.cmd.spawn();
    this.bindCmdEmitter(this.cmd);
    this.bindProcessEmitter(this.process);
    const processIn = this.process.stdin!!;
    const processOut = this.process.stdout!!;

    this.fileStream = await getFile(this.props.mediaInfo.location);
    await this.loadSubtitles(this.fileStream);

    const errorHandler = this.mkErrorHandler(processIn);
    this.fileStream.pipe(processIn).on("error", errorHandler);
    this.fileStream.on("error", errorHandler);

    processOut
      .pipe(this.m4f)
      .on("segment", this.mkSegmentHandler())
      .on("initialized", this.mkInitHandler())
      .on("error", errorHandler)
      .on("close", errorHandler);
  }

  private mkErrorHandler(processIn: Writable) {
    return (e: any) => {
      log.error(
        `Error reading file from ${JSON.stringify(
          this.props.mediaInfo.location
        )}`,
        e
      );
      processIn.end();
      // TODO fail stream
    };
  }

  // TODO specify the emitted type in @types/mp4frag
  private mkInitHandler() {
    return async (init: {
      mime: string;
      initialization: Buffer;
      m3u8: string;
    }) => {
      await Promise.all([
        await this.dataHelper.putInitMp4(init.initialization),
        await this.metadataHelper.setMime(Stream.parseMp4FragMime(init.mime)),
      ]);
      log.info(`MIME: ${init.mime}`);
    };
  }

  private mkSegmentHandler() {
    return async (segment: SegmentObject) => {
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
    };
  }

  private async loadSubtitles(readable: Readable) {
    if (this.props.subtitle) {
      (isFileLocation(this.props.subtitle)
        ? Stream.handleSubtitles(
            await getFile(this.props.subtitle),
            this.props.startOffset
          )
        : Stream.handleSubtitles(
            readable,
            this.props.startOffset
            // this.props.subtitle.index
          )
      ).then((subtitles) => this.metadataHelper.setSubtitles(subtitles));
    }
  }

  private bindProcessEmitter(process: ChildProcess) {
    process
      .on("exit", async (code) => {
        await this.endStream(code !== 0);
      })
      .on("close", async (code) => {
        await this.endStream(code !== 0);
      })
      .on("disconnect", async () => {
        // TODO do anything?
      })
      .on("error", async (err) => {
        log.error(`Stream ${this.props.id} failed`, err);
        await this.endStream(false);
      });
  }

  private bindCmdEmitter(cmd: FFmpegCommand) {
    cmd
      .on("exit", async (code) => {
        await this.endStream(code !== 0);
      })
      .on("close", async (code) => {
        await this.endStream(code !== 0);
      })
      .on("disconnect", async () => {
        // TODO do anything?
      })
      .on("error", async (err) => {
        log.error(`Stream ${this.props.id} failed`, err);
        await this.endStream(false);
      });
  }

  private async _destroy(): Promise<void> {
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

  async destroy(): Promise<void> {
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

  private static parseMp4FragMime(mime: string): Mime {
    const [typeStr, codecStr] = mime.split(";").map((it) => it.trim());
    const codecs = (codecStr ?? "")
      .replace("codecs=", "")
      .replaceAll('"', "")
      .split(",")
      .map((it) => it.trim())
      .filter((it) => it != "");
    return {
      mimeType: typeStr ?? "",
      codecs: codecs ?? [],
    };
  }

  private static async handleSubtitles(
    readable: Readable,
    startOffset: number
    // subtitleTrack: number = 0 //TODO get embedded subtitles working
  ) {
    const input = new ff.FFmpegInput("pipe:0", {});
    const output = new ff.FFmpegOutput("pipe:1", {
      f: "webvtt",
      ss: startOffset.toString(),
      // map: `${subtitleTrack}:s:0`
    });

    const cmd = new ff.FFmpegCommand();
    cmd.addInput(input);
    cmd.addOutput(output);
    cmd.on("error", (e) => {
      log.error("Failed to process file for subtitle extraction", e);
      readable.unpipe(process.stdin);
    });

    log.info(`Extracting subtitles with: ${cmd.toString()}`);
    const process = cmd.spawn();

    readable.pipe(process.stdin).on("error", (e) => {
      log.error("Failed to read file for subtitle extraction", e);
      readable.unpipe(process.stdin);
    });

    const subs = (await readFile(process.stdout)).toString("utf8");
    log.debug(`Subtitles: ${subs}`);
    return subs;
    // return (await readFile(readable)).toString("utf8");
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
      log.error(
        `Stream codec type 'subtitle' with name ${stream.codec_name} is not supported`
      );
      return undefined;
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
}
