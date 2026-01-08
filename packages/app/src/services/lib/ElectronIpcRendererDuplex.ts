import { Duplex } from 'stream';

const GET_CURRENT_WEB_CONTENTS_ID = 'stream-electron-ipc.get-current-web-contents-id';
const getFullChannel = (channel: string, webContentsId: number) => `sei-${channel}-${webContentsId}`;
const RELAY_CHANNEL = 'sei-relay';

// Shared listener registry to prevent duplicate IPC listeners per channel
const dataListeners = new Map<string, Set<ElectronIpcRendererDuplex>>();
const dataListenersInstalled = new Set<string>();

export class ElectronIpcRendererDuplex extends Duplex {
  private wcId: number;
  private sendTo: (channel: string, ...args: any[]) => void;
  private channel: string;
  private ipcRenderer: any;
  private incomingChannel: string;
  public isDestroyed = false;

  constructor(webContentsId?: number, channel: string = 'data') {
    super();

    const electron: any = require('electron');
    const ipcRenderer: any = electron.ipcRenderer;
    this.ipcRenderer = ipcRenderer;

    this.wcId = typeof webContentsId === 'number' ? webContentsId : 0;

    const currentWebContentsId = ipcRenderer.sendSync(GET_CURRENT_WEB_CONTENTS_ID);
    this.channel = getFullChannel(channel, currentWebContentsId);

    if (this.wcId === 0) {
      this.sendTo = ipcRenderer.send.bind(ipcRenderer);
    } else {
      this.sendTo = (outChannel: string, ...args: any[]) => {
        ipcRenderer.send(RELAY_CHANNEL, this.wcId, outChannel, ...args);
      };
    }

    this.incomingChannel = getFullChannel(channel, this.wcId);

    // Register this duplex to receive data from shared listener
    if (!dataListeners.has(this.incomingChannel)) {
      dataListeners.set(this.incomingChannel, new Set());
    }
    dataListeners.get(this.incomingChannel)!.add(this);

    // Install shared listener only once per channel
    if (!dataListenersInstalled.has(this.incomingChannel)) {
      dataListenersInstalled.add(this.incomingChannel);
      ipcRenderer.on(this.incomingChannel, (_: any, senderIdOrData: any, maybeData?: Uint8Array) => {
        // For relayed messages, first arg is sender webContentsId, second is data
        // For direct messages, first arg is data
        const data = typeof senderIdOrData === 'number' ? maybeData : senderIdOrData;
        const duplexes = dataListeners.get(this.incomingChannel);
        if (duplexes && data) {
          for (const duplex of duplexes) {
            if (!duplex.isDestroyed) {
              duplex.push(data);
            }
          }
        }
      });
    }

    const cleanup = () => {
      this.isDestroyed = true;
      const duplexes = dataListeners.get(this.incomingChannel);
      if (duplexes) {
        duplexes.delete(this);
      }
    };

    this.once('close', cleanup);
    this.once('finish', cleanup);
    this.once('end', cleanup);

    this.sendTo(channel);
  }

  _write(chunk: Buffer, _encoding: any, callback: Function) {
    this.sendTo(this.channel, new Uint8Array(chunk));
    callback();
  }

  _read(_size: any) {}
}
