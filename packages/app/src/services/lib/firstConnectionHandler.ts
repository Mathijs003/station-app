import { Duplex } from 'stream';

const GET_CURRENT_WEB_CONTENTS_ID = 'stream-electron-ipc.get-current-web-contents-id';
const getFullChannel = (channel: string, webContentsId: number) => `sei-${channel}-${webContentsId}`;

const isRenderer = process.type === 'renderer';
const getSenderId = (e: any) => typeof e.senderId === 'number' ? e.senderId :
  typeof e.sender?.id === 'number' ? e.sender.id : 0;

let mainInitialized = false;

// Shared listener registry to prevent duplicate IPC listeners per channel
const mainDataListeners = new Map<string, Set<ElectronIpcMainDuplex>>();
const mainDataListenersInstalled = new Set<string>();

class ElectronIpcMainDuplex extends Duplex {
  private webContents: Electron.WebContents;
  private wcId: number;
  private channel: string;
  private incomingChannel: string;
  private isDestroyed = false;

  constructor(webContents: Electron.WebContents, channel: string = 'data') {
    super();
    this.webContents = webContents;
    this.wcId = webContents.id;
    this.channel = getFullChannel(channel, 0);
    this.incomingChannel = getFullChannel(channel, this.wcId);

    // Register this duplex to receive data from shared listener
    if (!mainDataListeners.has(this.incomingChannel)) {
      mainDataListeners.set(this.incomingChannel, new Set());
    }
    mainDataListeners.get(this.incomingChannel)!.add(this);

    // Install shared listener only once per channel
    if (!mainDataListenersInstalled.has(this.incomingChannel)) {
      mainDataListenersInstalled.add(this.incomingChannel);
      const electron: any = require('electron');
      const ipcMain = electron.ipcMain;
      ipcMain.on(this.incomingChannel, (_: any, data: Uint8Array) => {
        const duplexes = mainDataListeners.get(this.incomingChannel);
        if (duplexes) {
          for (const duplex of duplexes) {
            if (!duplex.isDestroyed) {
              duplex.push(data);
            }
          }
        }
      });
    }

    (webContents as any).once('close', () => {
      this.end();
    });
    (webContents as any).once('destroyed', () => {
      this.cleanup();
      this.destroy();
    });

    // init connection
    this.webContents.send(channel);
  }

  private cleanup() {
    this.destroyed = true;
    const duplexes = mainDataListeners.get(this.incomingChannel);
    if (duplexes) {
      duplexes.delete(this);
    }
  }

  _write(chunk: Buffer, _encoding: any, callback: Function) {
    if (!this.webContents.isDestroyed()) {
      this.webContents.send(this.channel, new Uint8Array(chunk));
    }
    callback();
  }

  _read(_size: any) {}

  _destroy(error: Error | null, callback: (error: Error | null) => void) {
    this.cleanup();
    callback(error);
  }
}

// Shared listener registry for renderer to prevent duplicate IPC listeners per channel
const rendererDataListeners = new Map<string, Set<ElectronIpcRendererDuplex>>();
const rendererDataListenersInstalled = new Set<string>();

class ElectronIpcRendererDuplex extends Duplex {
  private wcId: number;
  private sendTo: (channel: string, ...args: any[]) => void;
  private channel: string;
  private ipcRenderer: any;
  private incomingChannel: string;
  public isDestroyed = false;

  constructor(webContentsId?: number, channel: string = 'data') {
    super();

    const electron: any = require('electron');
    this.ipcRenderer = electron.ipcRenderer;

    this.wcId = typeof webContentsId === 'number' ? webContentsId : 0;

    const currentWebContentsId = this.ipcRenderer.sendSync(GET_CURRENT_WEB_CONTENTS_ID);
    this.channel = getFullChannel(channel, currentWebContentsId);

    if (this.wcId === 0) {
      this.sendTo = this.ipcRenderer.send.bind(this.ipcRenderer);
    } else {
      // Use relay through main process for renderer-to-renderer
      this.sendTo = (outChannel: string, ...args: any[]) => {
        this.ipcRenderer.send('sei-relay', this.wcId, outChannel, ...args);
      };
    }

    this.incomingChannel = getFullChannel(channel, this.wcId);

    // Register this duplex to receive data from shared listener
    if (!rendererDataListeners.has(this.incomingChannel)) {
      rendererDataListeners.set(this.incomingChannel, new Set());
    }
    rendererDataListeners.get(this.incomingChannel)!.add(this);

    // Install shared listener only once per channel
    if (!rendererDataListenersInstalled.has(this.incomingChannel)) {
      rendererDataListenersInstalled.add(this.incomingChannel);
      this.ipcRenderer.on(this.incomingChannel, (_: any, senderIdOrData: any, maybeData?: Uint8Array) => {
        // For relayed messages, first arg is sender webContentsId, second is data
        // For direct messages, first arg is data
        const data = typeof senderIdOrData === 'number' ? maybeData : senderIdOrData;
        const duplexes = rendererDataListeners.get(this.incomingChannel);
        if (duplexes && data) {
          for (const duplex of duplexes) {
            if (!duplex.isDestroyed) {
              duplex.push(data);
            }
          }
        }
      });
    }

    // init connection
    this.sendTo(channel);
  }

  private cleanup() {
    this.isDestroyed = true;
    const duplexes = rendererDataListeners.get(this.incomingChannel);
    if (duplexes) {
      duplexes.delete(this);
    }
  }

  _write(chunk: Buffer, _encoding: any, callback: Function) {
    this.sendTo(this.channel, new Uint8Array(chunk));
    callback();
  }

  _read(_size: any) {}

  _destroy(error: Error | null, callback: (error: Error | null) => void) {
    this.cleanup();
    callback(error);
  }
}

// Track seen sender IDs and registered handlers per channel to prevent duplicate listeners
const seenIds = new Map<string, Set<number>>();
const registeredChannels = new Map<string, Array<(socket: Duplex) => void>>();
const channelListenersInstalled = new Set<string>();
// Track active duplexes per sender to reuse or cleanup
const activeDuplexes = new Map<string, Duplex>();

export const firstConnectionHandler = (callback: (socket: Duplex) => void, channel?: string) => {
  const channelKey = channel || 'data';
  
  if (!seenIds.has(channelKey)) {
    seenIds.set(channelKey, new Set<number>());
  }
  const seenIdsForChannel = seenIds.get(channelKey)!;

  // Register callback for this channel
  if (!registeredChannels.has(channelKey)) {
    registeredChannels.set(channelKey, []);
  }
  registeredChannels.get(channelKey)!.push(callback);

  // Only install one IPC listener per channel
  if (channelListenersInstalled.has(channelKey)) {
    return;
  }
  channelListenersInstalled.add(channelKey);

  if (isRenderer) {
    const electron: any = require('electron');
    const ipcRenderer = electron.ipcRenderer;

    const handler = (e: any, senderIdOrData: any, ...rest: any[]) => {
      // For relayed messages, first arg is the sender webContentsId
      // For direct messages, first arg is data
      let senderId: number;
      let data: any;
      if (typeof senderIdOrData === 'number') {
        senderId = senderIdOrData;
        data = rest[0];
      } else {
        senderId = getSenderId(e);
        data = senderIdOrData;
      }
      
      const duplexKey = `${channelKey}-${senderId}`;
      
      // Check if we already have an active duplex for this sender
      if (activeDuplexes.has(duplexKey)) {
        return;
      }
      
      if (!channel) {
        if (seenIdsForChannel.has(senderId)) return;
        seenIdsForChannel.add(senderId);
      }
      const duplex = new ElectronIpcRendererDuplex(senderId, channel || 'data');
      activeDuplexes.set(duplexKey, duplex);
      
      // Cleanup when duplex is destroyed
      duplex.once('close', () => activeDuplexes.delete(duplexKey));
      duplex.once('error', () => activeDuplexes.delete(duplexKey));
      
      if (!channel) {
        if (data !== undefined) {
          duplex.push(data);
        }
      }
      // Call all registered callbacks for this channel
      const callbacks = registeredChannels.get(channelKey) || [];
      for (const cb of callbacks) {
        cb(duplex);
      }
    };

    ipcRenderer.on(channel || 'data', handler);
  } else {
    const electron: any = require('electron');
    const ipcMain = electron.ipcMain;

    // Initialize the GET_CURRENT_WEB_CONTENTS_ID handler once
    if (!mainInitialized) {
      ipcMain.on(GET_CURRENT_WEB_CONTENTS_ID, (event: Electron.IpcMainEvent) => {
        event.returnValue = event.sender.id;
      });
      mainInitialized = true;
    }

    const handler = (e: any, data: any) => {
      const senderId = getSenderId(e);
      const duplexKey = `${channelKey}-${senderId}`;
      
      // Check if we already have an active duplex for this sender
      if (activeDuplexes.has(duplexKey)) {
        return;
      }
      
      if (!channel) {
        if (seenIdsForChannel.has(senderId)) return;
        seenIdsForChannel.add(senderId);
      }
      const duplex = new ElectronIpcMainDuplex(e.sender, channel || 'data');
      activeDuplexes.set(duplexKey, duplex);
      
      // Cleanup when duplex is destroyed
      duplex.once('close', () => activeDuplexes.delete(duplexKey));
      duplex.once('error', () => activeDuplexes.delete(duplexKey));
      
      if (!channel) {
        duplex.push(data);
      }
      // Call all registered callbacks for this channel
      const callbacks = registeredChannels.get(channelKey) || [];
      for (const cb of callbacks) {
        cb(duplex);
      }
    };

    ipcMain.on(channel || 'data', handler);
  }
};
