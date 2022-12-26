import { spawn } from "child_process";
import { FFProbeResult } from "ffprobe";
import { Stream } from "stream";
import { WorkerLogger } from "../log.js";

const log = WorkerLogger.getChildCategory("ffmpeg.probe");

export interface FfProbeInfoFormat {
  duration?: number;
  filename?: string;
}

export interface FfProbeInfo extends FFProbeResult {
  readonly format: FfProbeInfoFormat;
}

export function getInfo(filePath: string): Promise<FfProbeInfo> {
  return new Promise((resolve, reject) => {
    const params = [
      "-show_streams",
      "-show_programs",
      "-show_chapters",
      "-show_entries",
      "format=duration",
      "-print_format",
      "json",
      filePath,
    ];
    var out: string | undefined;
    var err: string | undefined;
    const succeed = () => {
      if (out) {
        resolve(JSON.parse(out) as FfProbeInfo);
      } else {
        reject(err);
      }
    };
    const fail = () => {
      reject(ffprobe.stderr.setEncoding("utf8").read());
    };
    const ffprobe = spawn("ffprobe", params);
    streamToString(ffprobe.stdout).then((str) => (out = str));
    streamToString(ffprobe.stderr).then((str) => (err = str));
    ffprobe.on("close", (code) => {
      if (!code) {
        succeed();
      } else {
        fail();
      }
    });
    ffprobe.on("error", () => {
      fail();
    });
    ffprobe.on("disconnect", () => {
      fail();
    });
  });
}

function streamToString(stream: Stream): Promise<string> {
  const chunks: Buffer[] = [];
  return new Promise((resolve) => {
    stream.on("data", (chunk) => {
      log.debug(chunk);
      chunks.push(Buffer.from(chunk));
    });
    stream.on("error", (err) => {
      log.debug(err);
    }); //reject(err));
    stream.on("end", () => {
      const res = Buffer.concat(chunks).toString("utf8");
      log.debug(res);
      resolve(res);
    });
  });
}
