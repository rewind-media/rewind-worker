import EventEmitter from "events";

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
