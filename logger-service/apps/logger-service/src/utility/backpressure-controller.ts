import EventEmitter from 'events';
import { BackpressureEventData } from '../interfaces/ibackpressure-controller';

class BackPressureController extends EventEmitter {
  private static instance: BackPressureController;
  //   private zsetlimit: number;

  static getInstance(): BackPressureController {
    if (!BackPressureController.instance) {
      BackPressureController.instance = new BackPressureController();
    }
    return BackPressureController.instance;
  }

  private constructor() {
    super();
    // this.zsetlimit = zsetLimit;
  }

  public emitQueuePause(serviceName: string) {
    const data: BackpressureEventData = {
      service: serviceName,
    };
    this.emit('pause', data);
  }

  public emitQueueResume(serviceName: string) {
    const data: BackpressureEventData = {
      service: serviceName,
    };
    this.emit('resume', data);
  }

  public subscribeToQueuePause(
    callback: (data: BackpressureEventData) => Promise<void> | void,
  ) {
    this.on('pause', async (data) => await callback(data));
  }

  public subscribeToQueueResume(
    callback: (data: BackpressureEventData) => Promise<void> | void,
  ) {
    this.on('resume', async (data) => await callback(data));
  }
}

export default BackPressureController;
