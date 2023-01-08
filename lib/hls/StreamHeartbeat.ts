import { Duration } from "durr";
import { clearInterval } from "timers";

export class StreamHeartbeat {
  lastHeartbeatTime = new Date();
  isDead = false;
  constructor(
    private onDeath: () => void,
    private timer: NodeJS.Timer = setInterval(() => {
      if (
        Duration.seconds(5).after(this.lastHeartbeatTime).getTime() < Date.now()
      ) {
        this.cancel(true);
      }
    }, Duration.seconds(5).millis)
  ) {}

  cancel(invokeOnDeath: boolean = false) {
    this.isDead = true;
    clearInterval(this.timer);
    if (invokeOnDeath) {
      this.onDeath();
    }
  }

  invoke() {
    this.lastHeartbeatTime = new Date();
  }
}
